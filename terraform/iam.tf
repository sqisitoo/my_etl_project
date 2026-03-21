# =============================================================================
# IAM.TF — ECS Roles & Policies for Airflow
# =============================================================================
# Two IAM roles are needed for ECS Fargate tasks:
#
#   1. Execution Role — used by the ECS *agent* (infrastructure-level).
#      Grants permissions to pull Docker images from ECR, write logs to
#      CloudWatch, and fetch secrets from Secrets Manager at container start.
#
#   2. Task Role — used by the *application code* running inside containers.
#      Grants permissions the Airflow DAGs and processes actually need at
#      runtime: S3 read/write (datalake), and ECS Exec (interactive shell).
#
# Both roles share the same trust policy (ecs-tasks.amazonaws.com), but
# have completely different permission sets.
# =============================================================================


# -----------------------------------------------------------------------------
# TRUST POLICY — who can assume these roles
# -----------------------------------------------------------------------------
# Shared by both the execution role and the task role.
# Only the ECS tasks service principal can assume them.
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}


# -----------------------------------------------------------------------------
# 1. EXECUTION ROLE — infrastructure-level permissions for the ECS agent
# -----------------------------------------------------------------------------
# The ECS agent (not your code) uses this role to:
#   - Pull container images from ECR
#   - Send logs to CloudWatch
#   - Fetch secrets from Secrets Manager and inject them as env vars
#
# AmazonECSTaskExecutionRolePolicy is the AWS-managed policy covering
# ECR pull + CloudWatch logs. Secrets Manager access is added separately
# because the managed policy doesn't include it.
# -----------------------------------------------------------------------------
resource "aws_iam_role" "ecs_execution_role" {
  name               = "airflow-ecs-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

# AWS-managed policy: ECR image pull + CloudWatch log writes.
resource "aws_iam_role_policy_attachment" "ecs_execution_role_policy" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Custom policy: allow the ECS agent to read specific Secrets Manager secrets
# at container start (these get injected as env vars via the "secrets" block
# in the task definition, see ecs.tf section 6).
resource "aws_iam_policy" "secrets_access" {
  name        = "airflow-secrets-access"
  description = "Allow reading secrets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [
          aws_secretsmanager_secret.airflow_core_secrets.arn,
          aws_secretsmanager_secret.api_key_secret.arn,
          aws_secretsmanager_secret.airflow_snowflake.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "execution_secrets" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = aws_iam_policy.secrets_access.arn
}


# -----------------------------------------------------------------------------
# 2. TASK ROLE — application-level permissions for Airflow code
# -----------------------------------------------------------------------------
# This role is assumed by the running containers themselves — i.e. your
# Airflow DAGs, scheduler, and apiserver use it when calling AWS APIs.
#
# Permissions granted:
#   - S3: read/write/delete objects in the datalake bucket (for ETL data
#     and Airflow remote logs).
#   - SSM Messages: allows `aws ecs execute-command` to open an interactive
#     shell into running containers (ECS Exec). Useful for debugging in a
#     pet project. In production, restrict this to specific IAM users/roles
#     and enable it only temporarily — ECS Exec gives root-level access
#     inside the container.
# -----------------------------------------------------------------------------
resource "aws_iam_role" "airflow_task_role" {
  name               = "airflow-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

# S3 access for the datalake bucket.
# ListBucket is scoped to the bucket itself (needed for `s3:ListObjects`).
# Object-level actions (Get/Put/Delete) are scoped to all keys inside it.
resource "aws_iam_policy" "s3_access" {
  name = "airflow-s3-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.datalake.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.datalake.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "task_s3" {
  role       = aws_iam_role.airflow_task_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}

# ECS Exec — allows interactive shell into running containers via:
#   aws ecs execute-command --cluster ... --task ... --container ... --interactive --command "/bin/bash"
# Requires enable_execute_command=true on the ECS service (see ecs.tf section 9).
# The ssmmessages:* actions are what the SSM agent inside the container needs.
resource "aws_iam_role_policy" "ecs_exec_policy" {
  role = aws_iam_role.airflow_task_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel"
        ]
        Resource = "*"
      }
    ]
  })
}


# -----------------------------------------------------------------------------
# OUTPUTS
# -----------------------------------------------------------------------------
output "ecr_repository_url" {
  value = aws_ecr_repository.airflow.repository_url
}
