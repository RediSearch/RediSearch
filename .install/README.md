Platform-specific install scripts in this folder are named and selected automatically by install_script.sh.

On macOS (`uname -s` == `Darwin`), the script sources macos.sh directly.

On Linux, the name is derived from /etc/os-release``:

  1. Read NAME and VERSION_ID (quotes stripped).
  2. Rocky Linux: strip the minor version from VERSION_ID (e.g. "9.3" -> "9").
     Alpine Linux: strip the minor and patch version (e.g. "3.22.1" -> "3").
  3. Lowercase NAME, append "_" and VERSION_ID.
  4. Replace all spaces and forward slashes with underscores.

Examples:

  NAME="Ubuntu"                   VERSION_ID="26.04"  ->  ubuntu_26.04.sh
  NAME="Debian GNU/Linux"         VERSION_ID="13"     ->  debian_gnu_linux_13.sh
  NAME="Rocky Linux"              VERSION_ID="10.0"   ->  rocky_linux_10.sh
  NAME="Alpine Linux"             VERSION_ID="3.22.1" ->  alpine_linux_3.sh
  NAME="Amazon Linux"             VERSION_ID="2023"   ->  amazon_linux_2023.sh
  NAME="Common Base Linux Mariner" VERSION_ID="2.0"   ->  common_base_linux_mariner_2.0.sh
  NAME="Microsoft Azure Linux"    VERSION_ID="3.0"    ->  microsoft_azure_linux_3.0.sh

When adding a new platform, boot the container and run:
  `grep '^NAME=\|^VERSION_ID=' /etc/os-release`
to determine the exact script name required.
