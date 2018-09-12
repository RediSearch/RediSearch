
function print_help_message {
	echo "this script run clang-format on modified files."
	echo "options : "
	echo "         --dry-run -                   check format only (without modify the files)"
	echo "         --install-pre-commit-script - install git pre commit script"
}

function install_pre_commit_script {
	cp ./pre-commit-script.sh ./.git/hooks/pre-commit	
}

if [ "$1" == "--help" ]; then 
	print_help_message
	exit 0
fi

if [ "$1" == "--install-pre-commit-script" ]; then
	install_pre_commit_script
	exit 0
fi

if [ "$1" == "--dry-run" ]; then

	RED='\033[0;31m'
	GREEN='\033[0;32m'
	NC='\033[0m' # No Color

	output=$(git status -s | awk -F' ' '{ print $2}' | grep -e "\.c" -e "\.h" -e "\.cpp" -e "\.hpp" | xargs clang-format -style=file -fallback-style=none -output-replacements-xml | grep -c "")

	if [ $output -lt 4 ]; then
		echo -e "style check: ${GREEN}successed${NC}"
		exit 0
	else
		echo -e "style check: ${RED}failed${NC}"
		echo "run code_style.sh to fix styling"
		exit 1
	fi
else
	git ls-files -om "src/*.[ch]" "src/*.[hc]pp" | xargs clang-format -style=file -fallback-style=none -i
	echo "files modified"
fi
