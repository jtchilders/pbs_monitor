#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path
import logging


def run(cmd: list[str]) -> str:
   try:
      out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8", errors="replace")
      return out
   except subprocess.CalledProcessError as e:
      return e.output.decode("utf-8", errors="replace")


def generate_cli_reference(output_path: Path) -> None:
   sections: list[str] = []

   sections.append("# CLI Reference\n")
   sections.append("This page is generated from `pbs-monitor --help` and subcommand help outputs.\n\n")

   # Top-level help
   sections.append("## Global Help\n\n")
   sections.append("```\n" + run(["pbs-monitor", "--help"]) + "```\n\n")

   # Known top-level commands to document
   top_commands = [
      "status",
      "jobs",
      "nodes",
      "queues",
      "history",
      "database",
      "daemon",
      "resv",
      "analyze",
   ]

   for cmd in top_commands:
      sections.append(f"## {cmd}\n\n")
      sections.append("```\n" + run(["pbs-monitor", cmd, "--help"]) + "```\n\n")

      # Known subcommands for certain groups
      if cmd == "database":
         for sub in ["init", "migrate", "status", "validate", "backup", "restore", "cleanup"]:
            sections.append(f"### database {sub}\n\n")
            sections.append("```\n" + run(["pbs-monitor", "database", sub, "--help"]) + "```\n\n")
      if cmd == "daemon":
         for sub in ["start", "stop", "status"]:
            sections.append(f"### daemon {sub}\n\n")
            sections.append("```\n" + run(["pbs-monitor", "daemon", sub, "--help"]) + "```\n\n")
      if cmd == "resv":
         for sub in ["list", "show"]:
            sections.append(f"### resv {sub}\n\n")
            sections.append("```\n" + run(["pbs-monitor", "resv", sub, "--help"]) + "```\n\n")
      if cmd == "analyze":
         for sub in ["run-score"]:
            sections.append(f"### analyze {sub}\n\n")
            sections.append("```\n" + run(["pbs-monitor", "analyze", sub, "--help"]) + "```\n\n")

   output_path.parent.mkdir(parents=True, exist_ok=True)
   output_path.write_text("".join(sections))


def main() -> int:
   parser = argparse.ArgumentParser(description="Generate CLI reference from pbs-monitor help outputs")
   parser.add_argument("--output", default=str(Path("docs/user/cli_reference.md")), help="Output markdown path")
   parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
   args = parser.parse_args()

   logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

   output_path = Path(args.output)
   generate_cli_reference(output_path)
   logging.info("Wrote CLI reference to %s", output_path)
   return 0


if __name__ == "__main__":
   sys.exit(main())
