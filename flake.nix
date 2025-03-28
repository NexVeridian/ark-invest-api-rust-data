{
  description = "Build a cargo project";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      crane,
      fenix,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        inherit (pkgs) lib;

        craneLib = (crane.mkLib pkgs).overrideToolchain (
          p:
          (
            let
              fp = fenix.packages.${system};
              fpc = fp.complete;
            in
            (fp.combine [
              fpc.cargo
              fpc.rustc
              fpc.clippy
              fpc.rust-src
              fpc.rustfmt
              fpc.rustc-codegen-cranelift-preview
            ])
          )
        );
        src = craneLib.cleanCargoSource ./.;

        # Common arguments can be set here to avoid repeating them later
        commonArgs = {
          inherit src;
          strictDeps = true;

          nativeBuildInputs = [
            pkgs.pkg-config
            pkgs.perl
          ];

          buildInputs =
            [
              pkgs.openssl
            ]
            ++ lib.optionals pkgs.stdenv.isDarwin [
              pkgs.libiconv
              pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
              pkgs.darwin.apple_sdk.frameworks.Security
            ];
          LD_LIBRARY_PATH = with pkgs; lib.makeLibraryPath [ openssl ];
        };

        # Build *just* the cargo dependencies, so we can reuse
        # all of that work (e.g. via cachix) when running in CI
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # Build the actual crate itself, reusing the dependency
        # artifacts from above.
        my-crate = craneLib.buildPackage (
          commonArgs
          // {
            doCheck = false;
            inherit cargoArtifacts;
          }
        );

        # Define the Docker image build
        dockerImage = pkgs.dockerTools.buildImage {
          name = "ark-invest-api-rust-data";
          tag = "latest";
          copyToRoot = [ my-crate ];
          config = {
            Cmd = [ "${my-crate}/bin/ark-invest-api-rust-data" ];
          };
        };
      in
      {
        checks = {
          # Build the crate as part of `nix flake check` for convenience
          inherit my-crate;

          # Run clippy (and deny all warnings) on the crate source,
          # again, reusing the dependency artifacts from above.
          #
          # Note that this is done as a separate derivation so that
          # we can block the CI if there are issues here, but not
          # prevent downstream consumers from building our crate by itself.
          my-crate-clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--workspace";
            }
          );

          # my-crate-doc = craneLib.cargoDoc (commonArgs // {
          #   inherit cargoArtifacts;
          # });

          # Check formatting
          my-crate-fmt = craneLib.cargoFmt {
            inherit src;
            cargoExtraArgs = "--all";
          };

          # Run tests with cargo-nextest
          # Consider setting `doCheck = false` on `my-crate` if you do not want
          # the tests to run twice
          my-crate-nextest = craneLib.cargoNextest (
            commonArgs
            // {
              inherit cargoArtifacts;
              partitions = 1;
              partitionType = "count";
              cargoNextestPartitionsExtraArgs = "--no-tests=pass --no-fail-fast -E 'all() - test(get_api) - kind(bin)'";
              cargoExtraArgs = "--workspace";
            }
          );
        };

        packages = {
          default = my-crate;
          inherit
            my-crate
            dockerImage
            ;
        };

        apps.default = flake-utils.lib.mkApp {
          drv = my-crate;
        };

        devShells.default = craneLib.devShell {
          # Inherit inputs from checks.
          checks = self.checks.${system};

          # Additional dev-shell environment variables can be set directly
          # MY_CUSTOM_DEVELOPMENT_VAR = "something else";

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = [
            # pkgs.ripgrep
          ];
        };
      }
    );
}
