#!/bin/bash

# 🌿 Detect current working branch (main or master)
branch=""
if git branch --list | grep -q "main"; then
    branch="main"
elif git branch --list | grep -q "master"; then
    branch="master"
else
    echo "❌ Not found branch 'main' or 'master'."
    exit 1
fi

cyan=$(tput setaf 6)
bold=$(tput bold)
reset=$(tput sgr0)

# 🌟 Show current branch
echo -e "\n🌿 Current working branch: ${bold}${cyan}${branch}${reset}"

# 🔼 Push code function
function push_code() {
    git add .
    user=$(whoami)
    default_msg="$(hostname), $(date '+%H:%M:%S %d/%m/%Y') by $user"
    read -p "💬 Enter commit message (press Enter to use default): " msg

    if [[ -z "$msg" ]]; then
        commit_msg="$default_msg"
    else
        commit_msg="$msg ($(date '+%H:%M:%S %d/%m/%Y') by $user)"
    fi

    echo "📝 Committing: $commit_msg"
    sleep 1
    git commit -m "$commit_msg"
    sleep 1
    git push --force --set-upstream origin "$branch"
}

# 🔽 Pull code
function pull_code() {
    git fetch
    git reset --hard origin/"$branch"
}

# 🔍 Check diff
function check_diff() {
    git fetch
    git diff origin/"$branch"
}

# 📜 View commit log
function check_commit() {
    git --no-pager log --oneline --graph -n 10
}

# 🗑️ Delete commit
function delete_commit() {
    echo "📥 Fetching the latest commits..."
    git --no-pager log --oneline --graph -n 10
    read -p "🔑 Enter the commit hash you want to delete: " commit_hash
    if git rev-parse "$commit_hash" >/dev/null 2>&1; then
        if [ "$(git rev-list --max-parents=0 HEAD)" = "$commit_hash" ]; then
            echo "⚠️ Root commit detected. Using rebase --root..."
            git rebase --root
        else
            echo "🧬 Rebasing to remove commit..."
            git rebase --onto "$commit_hash"^ "$commit_hash"
        fi
        echo "✅ Commit $commit_hash has been removed. Please verify the changes."
    else
        echo "❌ Invalid commit hash. Please try again."
    fi
}

# 🧽 Reset to specific commit
function reset_commit() {
    echo "🧾 Recent commit list:"
    git --no-pager log --oneline --graph -n 10
    echo ""
    read -p "🔁 Enter the commit ID you want to reset to: " reset_id

    if git rev-parse "$reset_id" >/dev/null 2>&1; then
        read -p "⚠️ Are you sure you want to reset --hard to $reset_id? (y/N): " confirm
        if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
            git reset --hard "$reset_id"
            echo "✅ Reset to commit $reset_id"
        else
            echo "❎ Canceled reset."
        fi
    else
        echo "❌ Invalid commit ID. Please try again."
    fi
}

# 📋 Menu
echo -e "\n🛠️  Please select an option:"
echo -e "  1️⃣  Push code to remote"
echo -e "  2️⃣  Pull latest code"
echo -e "  3️⃣  View diff with origin/$branch"
echo -e "  4️⃣  Show recent commits"
echo -e "  5️⃣  Reset to a specific commit (hard)"
echo -e "  6️⃣  Exit"

read -p "👉 Enter your choice: " choice
echo ""

case $choice in
    1)
        echo "🚀 Executing push code..."
        push_code
        ;;
    2)
        echo "📥 Executing pull code..."
        pull_code
        ;;
    3)
        echo "🔍 Showing diff with origin/$branch..."
        check_diff
        ;;
    4)
        echo "🧾 Showing recent commits..."
        check_commit
        ;;
    5)
        echo "🔁 Resetting to a specific commit..."
        reset_commit
        ;;
    6)
        echo "👋 Exiting the program. See you soon!"
        exit 0
        ;;
    *)
        echo "❌ Invalid choice. Please try again."
        ;;
esac