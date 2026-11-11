#!/bin/bash
echo "=== Starting Deliverable 2 Push Automation ==="

# 1. Clear any stuck or bad Git credential caches
git config --global --unset credential.helper 2>/dev/null
git credential-cache exit 2>/dev/null

# 2. Ask for your valid Personal Access Token (with 'repo' scope)
read -sp "Enter your GitHub Personal Access Token (PAT): " GH_TOKEN
echo ""

if [ -z "$GH_TOKEN" ]; then
    echo "Error: Token cannot be empty."
    exit 1
fi

# 3. Set remote URL embedding the token directly
REMOTE_URL="https://${GH_TOKEN}@github.com/NandanNair/pubsub-sdn-project.git"
git remote set-url origin "$REMOTE_URL"

# 4. Ensure current branch is main and force push Deliverable 2
git branch -M main
echo "Pushing code to NandanNair/pubsub-sdn-project..."
git push -u origin main --force

if [ $? -eq 0 ]; then
    echo "=== SUCCESS! Deliverable 2 pushed successfully. ==="
else
    echo "=== PUSH FAILED ==="
    echo "If it failed with 403, your token lacks the 'repo' scope, or Nandan's collaborator invite hasn't been accepted at: https://github.com/NandanNair/pubsub-sdn-project/invitations"
fi
