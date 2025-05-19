{
  description = "Build a RediSearch C program that links to a Rust library";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
      in
      {
        devShells.default =  pkgs.mkShell {
          # Shell hooks to create executable scripts in a local bin directory
          shellHook = ''
            cargo_version=$(cargo --version 2>/dev/null)

            echo -e "\033[1;36m=== 🦀 Welcome to the RediSearch development environment ===\033[0m"
            echo -e "\033[1;33m• $cargo_version\033[0m"
            echo ""

            echo -e "\n\033[1;33m• Checking for any outdated packages...\033[0m\n"
            cd src/redisearch_rs && cargo outdated --root-deps-only

            # For libclang dependency to work
            export LIBCLANG_PATH="${pkgs.llvmPackages.libclang.lib}/lib"
            # For `sys/types.h` required by redismodules-rs
            export BINDGEN_EXTRA_CLANG_ARGS="-I${pkgs.glibc.dev}/include -I${pkgs.gcc-unwrapped}/lib/gcc/x86_64-unknown-linux-gnu/14.2.1/include"
          '';

          buildInputs = with pkgs; [
            # Dev dependencies based on developer.md
            cmake
            openssl.dev
            libxcrypt

            rust-bin.stable.latest.default
          ];

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = with pkgs; [
            rust-analyzer
            cargo-watch
            cargo-outdated
          ];
        };
      });
}
