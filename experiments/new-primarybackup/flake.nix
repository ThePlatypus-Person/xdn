{
  description = "Thesis development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
	config = {
          allowUnfree = true;
        };
      };

      pythonEnv = pkgs.python3.withPackages (ps: with ps; [
        gurobipy
	numpy
	matplotlib
	pandas
      ]);
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          pkgs.typst
	  pkgs.dejavu_fonts

	  pkgs.gurobi
          pythonEnv
        ];

	shellHook = ''
          export GUROBI_HOME=${pkgs.gurobi}
          export GRB_LICENSE_FILE=$HOME/Scripts/gurobi.lic
        '';
      };
    };
}
