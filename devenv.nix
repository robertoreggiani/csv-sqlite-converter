{ pkgs, lib, config, inputs, ... }:

{
  packages = [
    pkgs.csvs-to-sqlite
    pkgs.sqlite
    pkgs.csvkit
    pkgs.python3
  ];
}
