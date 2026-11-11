#!/bin/bash
git config --global --unset credential.helper 2>/dev/null
git credential-cache exit 2>/dev/null

read -sp "Paste your NEW Personal Access Token: " NEW_PAT
echo ""

if [ -z "$NEW_PAT" ]; then
    echo "Error: Token cannot be empty."
    exit 1
fi

git remote set-url origin "https://${NEW_PAT}@github.com/NandanNair/pubsub-sdn-project.git"
git branch -M main
echo "Pushing Deliverable 2..."
git push -u origin main --force

if [ $? -eq 0 ]; then
    echo "SUCCESS: Deliverable 2 pushed successfully to NandanNair/pubsub-sdn-project!"
else
    echo "PUSH FAILED."
fi
