#!/bin/bash
# =============================================================
# Setup IAM read-only users — Towerbank data team
# AWS Account: 720440144638 (innovacion)
# Profile: innovacion
# Usage: bash infra/setup_team_iam.sh
# =============================================================

AWS_PROFILE="innovacion"
ACCOUNT_ID="720440144638"
POLICY_NAME="twb-data-team-readonly"
GROUP_NAME="twb-data-team"
POLICY_FILE="infra/iam_readonly_policy.json"

# ── Team member list ────────────────────────────────────────────────────────────────
# Edit these names before running
TEAM_MEMBERS=(
  "data-analyst-1"
  "data-analyst-2"
  "data-engineer-1"
)

echo "============================================================"
echo " Towerbank — Setup IAM data team (read-only)"
echo " Account: $ACCOUNT_ID | Profile: $AWS_PROFILE"
echo "============================================================"
echo ""

# ── 1. Create read-only IAM policy ───────────────────────────────────────────────
echo "==> [1/3] Creating IAM policy: $POLICY_NAME"

EXISTING_POLICY=$(aws iam list-policies \
  --profile "$AWS_PROFILE" \
  --scope Local \
  --query "Policies[?PolicyName=='$POLICY_NAME'].Arn" \
  --output text)

if [ -n "$EXISTING_POLICY" ]; then
  echo "    Policy already exists: $EXISTING_POLICY"
  POLICY_ARN="$EXISTING_POLICY"
else
  POLICY_ARN=$(aws iam create-policy \
    --profile "$AWS_PROFILE" \
    --policy-name "$POLICY_NAME" \
    --policy-document file://$POLICY_FILE \
    --description "Read-only access to Towerbank DWH: Redshift, S3, Glue, CloudFormation" \
    --query "Policy.Arn" \
    --output text)
  echo "    Policy created: $POLICY_ARN"
fi
echo ""

# ── 2. Create IAM group and attach policy ──────────────────────────────────────────
echo "==> [2/3] Configuring IAM group: $GROUP_NAME"

aws iam create-group \
  --profile "$AWS_PROFILE" \
  --group-name "$GROUP_NAME" 2>/dev/null && echo "    Group created: $GROUP_NAME" || echo "    Group already exists: $GROUP_NAME"

aws iam attach-group-policy \
  --profile "$AWS_PROFILE" \
  --group-name "$GROUP_NAME" \
  --policy-arn "$POLICY_ARN"
echo "    Policy attached to group"
echo ""

# ── 3. Create users and generate Access Keys ─────────────────────────────────────────
echo "==> [3/3] Creating users..."
echo ""

OUTPUT_FILE="infra/team_credentials_$(date +%Y%m%d_%H%M%S).txt"
echo "Towerbank data team credentials — $(date)" > "$OUTPUT_FILE"
echo "Account: $ACCOUNT_ID | Region: us-east-1" >> "$OUTPUT_FILE"
echo "========================================" >> "$OUTPUT_FILE"

for USERNAME in "${TEAM_MEMBERS[@]}"; do
  echo "--- User: $USERNAME ---"

  aws iam create-user \
    --profile "$AWS_PROFILE" \
    --user-name "$USERNAME" \
    --tags Key=Team,Value=data Key=Access,Value=readonly Key=Project,Value=twb-dwh \
    2>/dev/null && echo "    User created" || echo "    User already exists"

  aws iam add-user-to-group \
    --profile "$AWS_PROFILE" \
    --user-name "$USERNAME" \
    --group-name "$GROUP_NAME"
  echo "    Added to group: $GROUP_NAME"

  KEYS=$(aws iam create-access-key \
    --profile "$AWS_PROFILE" \
    --user-name "$USERNAME" \
    --query "AccessKey.[AccessKeyId,SecretAccessKey]" \
    --output text)

  ACCESS_KEY_ID=$(echo "$KEYS" | awk '{print $1}')
  SECRET_KEY=$(echo "$KEYS" | awk '{print $2}')

  echo ""
  echo "    ┌──────────────────────────────────────────────────────────────┐"
  echo "    │ User:                  $USERNAME"
  echo "    │ AWS_ACCESS_KEY_ID:     $ACCESS_KEY_ID"
  echo "    │ AWS_SECRET_ACCESS_KEY: $SECRET_KEY"
  echo "    │ Region:                us-east-1"
  echo "    └──────────────────────────────────────────────────────────────┘"
  echo "    ⚠️  Save these credentials — they cannot be recovered later"
  echo ""

  echo "" >> "$OUTPUT_FILE"
  echo "User: $USERNAME" >> "$OUTPUT_FILE"
  echo "AWS_ACCESS_KEY_ID: $ACCESS_KEY_ID" >> "$OUTPUT_FILE"
  echo "AWS_SECRET_ACCESS_KEY: $SECRET_KEY" >> "$OUTPUT_FILE"
  echo "Region: us-east-1" >> "$OUTPUT_FILE"
  echo "----------------------------------------" >> "$OUTPUT_FILE"
done

echo "============================================================"
echo " ✅ Setup complete"
echo " Credentials saved to: $OUTPUT_FILE"
echo "============================================================"
