{
  description = "Application packaged using poetry2nix";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay.url = "github:oxalica/rust-overlay";
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix, rust-overlay, crane }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
        python = pkgs.python310;
        mkLETSQL = import ./nix/letsql.nix { inherit system pkgs crane poetry2nix python; };
        letsql = mkLETSQL ./.;

        commands = let
          get-first-pname-drv = pname: builtins.elemAt (builtins.filter (drv: drv.pname == pname) letsql.appFromWheel.requiredPythonModules) 0;
          black = get-first-pname-drv "black";
          blackdoc = get-first-pname-drv "blackdoc";
          ruff = get-first-pname-drv "ruff";
        in import ./nix/commands.nix {
          inherit pkgs python black blackdoc ruff;
        };
        inherit (commands) letsql-commands;

        toolsPackages = pkgs.buildEnv {
          name = "tools";
          paths = [
            letsql.toolchain
            pkgs.maturin
            pkgs.poetry
            python
          ] ++ (builtins.attrValues letsql-commands);
        };
        shellHook = ''
          export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
          ${letsql-commands.letsql-ensure-download-data}/bin/letsql-ensure-download-data
        '';
      in
      {
        packages = {
          inherit (letsql) app appFromWheel;
          inherit toolsPackages;
          default = self.packages.${system}.app;
        };
        lib = {
          inherit (letsql) poetryOverrides maturinOverride mkLETSQL;
        };
        devShells = {
          dev = pkgs.mkShell {
            packages = [
              self.packages.${system}.app
              toolsPackages
            ];
            inherit shellHook;
          };
          devFromWheel = pkgs.mkShell {
            packages = [
              self.packages.${system}.appFromWheel
              toolsPackages
            ];
            inherit shellHook;
          };
          inputs = pkgs.mkShell {
            inputsFrom = [ self.packages.${system}.app ];
            packages = toolsPackages;
            inherit shellHook;
          };
          tools = pkgs.mkShell {
            packages = [
              toolsPackages
            ];
            inherit shellHook;
          };
          default = self.devShells.${system}.devFromWheel;
        };
      });
}
