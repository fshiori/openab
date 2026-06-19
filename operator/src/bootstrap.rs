use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs::Client as LogsClient;
use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_ecs::Client as EcsClient;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sts::Client as StsClient;
use serde::{Deserialize, Serialize};

const CLUSTER_NAME: &str = "oab";
const EXECUTION_ROLE: &str = "oab-task-execution";
const TASK_ROLE: &str = "oab-task-role";
const SG_NAME: &str = "oab-agents";
const LOG_GROUP: &str = "/oab/agents";
const STATE_KEY: &str = "bootstrap/state.json";

const ASSUME_ROLE_POLICY: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}"#;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapState {
    pub version: u32,
    pub account: String,
    pub region: String,
    pub bucket: String,
    pub resources: BootstrapResources,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapResources {
    pub cluster_arn: String,
    pub execution_role_arn: String,
    pub task_role_arn: String,
    pub security_group_id: String,
    pub log_group: String,
    pub subnets: Vec<String>,
    pub vpc_id: String,
}

pub async fn run(config: &aws_config::SdkConfig, delete: bool, status: bool) -> Result<()> {
    if status {
        return show_status(config).await;
    }
    if delete {
        return teardown(config).await;
    }
    create(config).await
}

async fn get_account_and_region(config: &aws_config::SdkConfig) -> Result<(String, String)> {
    let sts = StsClient::new(config);
    let identity = sts.get_caller_identity().send().await?;
    let account = identity.account().context("no account ID")?.to_string();
    let region = config.region().map(|r| r.to_string()).unwrap_or_else(|| "us-east-1".to_string());
    Ok((account, region))
}

fn bucket_name(account: &str) -> String {
    format!("oab-control-plane-{account}")
}

async fn load_state(s3: &S3Client, bucket: &str) -> Result<Option<BootstrapState>> {
    match s3.get_object().bucket(bucket).key(STATE_KEY).send().await {
        Ok(resp) => {
            let bytes = resp.body.collect().await?.into_bytes();
            let state: BootstrapState = serde_json::from_slice(&bytes)?;
            Ok(Some(state))
        }
        Err(_) => Ok(None),
    }
}

/// Public accessor for other modules
pub async fn load_state_pub(s3: &S3Client, bucket: &str) -> Result<Option<BootstrapState>> {
    load_state(s3, bucket).await
}

async fn save_state(s3: &S3Client, bucket: &str, state: &BootstrapState) -> Result<()> {
    let json = serde_json::to_string_pretty(state)?;
    s3.put_object()
        .bucket(bucket)
        .key(STATE_KEY)
        .body(json.into_bytes().into())
        .content_type("application/json")
        .send()
        .await
        .context("failed to save bootstrap state")?;
    Ok(())
}

// ─── CREATE ───────────────────────────────────────────────────────────────────

async fn create(config: &aws_config::SdkConfig) -> Result<()> {
    let (account, region) = get_account_and_region(config).await?;
    let bucket = bucket_name(&account);

    eprintln!("🚀 Bootstrapping OAB infrastructure in {region} (account: {account})...\n");

    let ecs = EcsClient::new(config);
    let iam = IamClient::new(config);
    let s3 = S3Client::new(config);
    let ec2 = Ec2Client::new(config);
    let logs = LogsClient::new(config);

    // 1. S3 Bucket
    if s3.head_bucket().bucket(&bucket).send().await.is_ok() {
        eprintln!("  ✓ S3 bucket already exists: {bucket}");
    } else {
        let mut req = s3.create_bucket().bucket(&bucket);
        if region != "us-east-1" {
            req = req.create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(region.parse().unwrap())
                    .build(),
            );
        }
        req.send().await.context("failed to create S3 bucket")?;
        // Block public access
        s3.put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(
                aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
                    .block_public_acls(true)
                    .ignore_public_acls(true)
                    .block_public_policy(true)
                    .restrict_public_buckets(true)
                    .build(),
            )
            .send().await.ok();
        eprintln!("  ✓ Created S3 bucket: {bucket} (public access blocked)");
    }

    // 2. ECS Cluster — save state incrementally after this point
    let cluster_arn = match ecs.describe_clusters().clusters(CLUSTER_NAME).send().await {
        Ok(resp) if resp.clusters().first().is_some_and(|c| c.status() == Some("ACTIVE")) => {
            let arn = resp.clusters()[0].cluster_arn().unwrap_or_default().to_string();
            eprintln!("  ✓ ECS cluster already exists: {CLUSTER_NAME}");
            arn
        }
        _ => {
            let resp = ecs.create_cluster()
                .cluster_name(CLUSTER_NAME)
                .capacity_providers("FARGATE")
                .capacity_providers("FARGATE_SPOT")
                .default_capacity_provider_strategy(
                    aws_sdk_ecs::types::CapacityProviderStrategyItem::builder()
                        .capacity_provider("FARGATE_SPOT")
                        .weight(1)
                        .build()?,
                )
                .send()
                .await
                .context("failed to create ECS cluster")?;
            let arn = resp.cluster().and_then(|c| c.cluster_arn()).unwrap_or_default().to_string();
            eprintln!("  ✓ Created ECS cluster: {CLUSTER_NAME}");
            arn
        }
    };

    // 3. IAM Execution Role
    let execution_role_arn = ensure_role(&iam, EXECUTION_ROLE, &account).await?;

    // Save partial state (in case subsequent steps fail)
    let mut state = BootstrapState {
        version: 1,
        account: account.clone(),
        region: region.clone(),
        bucket: bucket.clone(),
        resources: BootstrapResources {
            cluster_arn: cluster_arn.clone(),
            execution_role_arn: execution_role_arn.clone(),
            task_role_arn: String::new(),
            security_group_id: String::new(),
            log_group: String::new(),
            subnets: vec![],
            vpc_id: String::new(),
        },
        created_at: chrono_now(),
    };
    save_state(&s3, &bucket, &state).await.ok();
    iam.attach_role_policy()
        .role_name(EXECUTION_ROLE)
        .policy_arn("arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy")
        .send()
        .await
        .ok(); // ignore if already attached
    eprintln!("  ✓ IAM execution role: {EXECUTION_ROLE}");

    // 4. IAM Task Role
    let task_role_arn = ensure_role(&iam, TASK_ROLE, &account).await?;
    // ECS Exec permissions
    iam.put_role_policy()
        .role_name(TASK_ROLE)
        .policy_name("oab-ecs-exec")
        .policy_document(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ssmmessages:CreateControlChannel","ssmmessages:CreateDataChannel","ssmmessages:OpenControlChannel","ssmmessages:OpenDataChannel"],"Resource":"*"}]}"#)
        .send().await.ok();
    // S3 access
    let s3_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":["arn:aws:s3:::{bucket}/*","arn:aws:s3:::ecsctl-staging-{account}/*"]}}]}}"#
    );
    iam.put_role_policy()
        .role_name(TASK_ROLE)
        .policy_name("oab-s3-access")
        .policy_document(&s3_policy)
        .send().await.ok();
    eprintln!("  ✓ IAM task role: {TASK_ROLE}");

    // 5. Security Group
    let default_vpc = ec2.describe_vpcs()
        .filters(aws_sdk_ec2::types::Filter::builder().name("isDefault").values("true").build())
        .send().await?;
    let vpc_id = default_vpc.vpcs().first()
        .and_then(|v| v.vpc_id())
        .context("no default VPC found")?
        .to_string();

    let sg_id = match ec2.describe_security_groups()
        .filters(aws_sdk_ec2::types::Filter::builder().name("group-name").values(SG_NAME).build())
        .filters(aws_sdk_ec2::types::Filter::builder().name("vpc-id").values(&vpc_id).build())
        .send().await
    {
        Ok(resp) if !resp.security_groups().is_empty() => {
            let id = resp.security_groups()[0].group_id().unwrap_or_default().to_string();
            eprintln!("  ✓ Security group already exists: {id}");
            id
        }
        _ => {
            let resp = ec2.create_security_group()
                .group_name(SG_NAME)
                .description("OAB agent containers — managed by oabctl bootstrap")
                .vpc_id(&vpc_id)
                .send().await
                .context("failed to create security group")?;
            let id = resp.group_id().unwrap_or_default().to_string();
            eprintln!("  ✓ Created security group: {id}");
            id
        }
    };

    // 6. Subnets (all default VPC subnets)
    let subnets_resp = ec2.describe_subnets()
        .filters(aws_sdk_ec2::types::Filter::builder().name("vpc-id").values(&vpc_id).build())
        .send().await?;
    let subnets: Vec<String> = subnets_resp.subnets().iter()
        .filter_map(|s| s.subnet_id().map(|id| id.to_string()))
        .collect();

    // 7. CloudWatch Log Group
    match logs.create_log_group().log_group_name(LOG_GROUP).send().await {
        Ok(_) => eprintln!("  ✓ Created log group: {LOG_GROUP}"),
        Err(_) => eprintln!("  ✓ Log group already exists: {LOG_GROUP}"),
    }

    // 8. Save final state
    state.resources.task_role_arn = task_role_arn;
    state.resources.security_group_id = sg_id;
    state.resources.log_group = LOG_GROUP.to_string();
    state.resources.subnets = subnets;
    state.resources.vpc_id = vpc_id;
    save_state(&s3, &bucket, &state).await?;

    eprintln!("\n✅ Bootstrap complete!");
    eprintln!("   State saved to: s3://{bucket}/{STATE_KEY}");
    eprintln!("   You can now run: oabctl apply -f <manifest.yaml>");
    Ok(())
}

// ─── DELETE ───────────────────────────────────────────────────────────────────

async fn teardown(config: &aws_config::SdkConfig) -> Result<()> {
    let (account, _region) = get_account_and_region(config).await?;
    let bucket = bucket_name(&account);
    let s3 = S3Client::new(config);

    let state = load_state(&s3, &bucket).await?
        .context("no bootstrap state found — nothing to delete")?;

    eprintln!("🗑️  Tearing down OAB bootstrap resources...\n");

    let ecs = EcsClient::new(config);
    let iam = IamClient::new(config);
    let ec2 = Ec2Client::new(config);
    let logs = LogsClient::new(config);

    // Check no running services
    let services = ecs.list_services().cluster(CLUSTER_NAME).send().await;
    if let Ok(resp) = &services {
        if !resp.service_arns().is_empty() {
            anyhow::bail!(
                "Cannot delete bootstrap: {} services still running on cluster '{}'. Delete them first.",
                resp.service_arns().len(),
                CLUSTER_NAME
            );
        }
    }

    // Reverse order
    // 1. Log group
    match logs.delete_log_group().log_group_name(LOG_GROUP).send().await {
        Ok(_) => eprintln!("  ✓ Deleted log group: {LOG_GROUP}"),
        Err(e) => eprintln!("  ⚠ Failed to delete log group: {e}"),
    }

    // 2. Security group
    match ec2.delete_security_group().group_id(&state.resources.security_group_id).send().await {
        Ok(_) => eprintln!("  ✓ Deleted security group: {}", state.resources.security_group_id),
        Err(e) => eprintln!("  ⚠ Failed to delete security group (may have attached ENIs): {e}"),
    }

    // 3. IAM roles
    delete_role(&iam, TASK_ROLE).await;
    eprintln!("  ✓ Deleted IAM role: {TASK_ROLE}");
    delete_role(&iam, EXECUTION_ROLE).await;
    eprintln!("  ✓ Deleted IAM role: {EXECUTION_ROLE}");

    // 4. ECS Cluster
    match ecs.delete_cluster().cluster(CLUSTER_NAME).send().await {
        Ok(_) => eprintln!("  ✓ Deleted ECS cluster: {CLUSTER_NAME}"),
        Err(e) => eprintln!("  ⚠ Failed to delete cluster: {e}"),
    }

    // 5. Delete state file (keep bucket for user data)
    s3.delete_object().bucket(&bucket).key(STATE_KEY).send().await.ok();
    eprintln!("  ✓ Deleted bootstrap state");
    eprintln!("\n  ℹ️  S3 bucket '{bucket}' preserved (may contain manifests/config).");
    eprintln!("     To fully remove: aws s3 rb s3://{bucket} --force");

    eprintln!("\n✅ Bootstrap teardown complete.");
    Ok(())
}

// ─── STATUS ───────────────────────────────────────────────────────────────────

async fn show_status(config: &aws_config::SdkConfig) -> Result<()> {
    let (account, _region) = get_account_and_region(config).await?;
    let bucket = bucket_name(&account);
    let s3 = S3Client::new(config);

    match load_state(&s3, &bucket).await? {
        Some(state) => {
            eprintln!("✅ OAB Bootstrap Status\n");
            eprintln!("  Account:        {}", state.account);
            eprintln!("  Region:         {}", state.region);
            eprintln!("  Created:        {}", state.created_at);
            eprintln!("  Bucket:         {}", state.bucket);
            eprintln!("  Cluster:        {}", state.resources.cluster_arn);
            eprintln!("  Execution Role: {}", state.resources.execution_role_arn);
            eprintln!("  Task Role:      {}", state.resources.task_role_arn);
            eprintln!("  Security Group: {}", state.resources.security_group_id);
            eprintln!("  Log Group:      {}", state.resources.log_group);
            eprintln!("  VPC:            {}", state.resources.vpc_id);
            eprintln!("  Subnets:        {}", state.resources.subnets.join(", "));
        }
        None => {
            eprintln!("❌ No bootstrap state found.");
            eprintln!("   Run: oabctl bootstrap");
        }
    }
    Ok(())
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────

async fn ensure_role(iam: &IamClient, name: &str, _account: &str) -> Result<String> {
    match iam.get_role().role_name(name).send().await {
        Ok(resp) => Ok(resp.role().arn().to_string()),
        Err(_) => {
            let resp = iam.create_role()
                .role_name(name)
                .assume_role_policy_document(ASSUME_ROLE_POLICY)
                .send().await
                .with_context(|| format!("failed to create role {name}"))?;
            Ok(resp.role().arn().to_string())
        }
    }
}

async fn delete_role(iam: &IamClient, name: &str) {
    // Detach managed policies
    if let Ok(resp) = iam.list_attached_role_policies().role_name(name).send().await {
        for p in resp.attached_policies() {
            if let Some(arn) = p.policy_arn() {
                iam.detach_role_policy().role_name(name).policy_arn(arn).send().await.ok();
            }
        }
    }
    // Delete inline policies
    if let Ok(resp) = iam.list_role_policies().role_name(name).send().await {
        for p in resp.policy_names() {
            iam.delete_role_policy().role_name(name).policy_name(p).send().await.ok();
        }
    }
    iam.delete_role().role_name(name).send().await.ok();
}

fn chrono_now() -> String {
    // UTC timestamp without chrono crate (YYYY-MM-DDTHH:MM:SSZ approximation)
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Convert to simple UTC string
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let mins = (time_secs % 3600) / 60;
    let s = time_secs % 60;
    // Days since 1970-01-01 to Y-M-D (simplified)
    let mut y = 1970u64;
    let mut remaining = days;
    loop {
        let days_in_year = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) { 366 } else { 365 };
        if remaining < days_in_year { break; }
        remaining -= days_in_year;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let month_days: [u64; 12] = [31, if leap {29} else {28}, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut m = 0;
    for md in month_days {
        if remaining < md { break; }
        remaining -= md;
        m += 1;
    }
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m + 1, remaining + 1, hours, mins, s)
}
