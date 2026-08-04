import subprocess
import sys

STEPS = {
    "generate": "generate_data.py",
    "etl": "etl.py",
    "analytics": "analytics.py",
}

def run(script):
    print(f"\n========== Running {script} ==========\n", flush=True)
    subprocess.run(
        ["python", f"src/{script}"],
        check=True
    )

if len(sys.argv) == 1:
    # Full pipeline
    run("generate_data.py")
    run("etl.py")
    run("analytics.py")

else:
    command = sys.argv[1]

    if command not in STEPS:
        print("Usage:")
        print("docker run financial-pipeline")
        print("docker run financial-pipeline generate")
        print("docker run financial-pipeline etl")
        print("docker run financial-pipeline analytics")
        print("\n===================================")
        print("Pipeline completed successfully!")
        print("===================================")
        sys.exit(1)

    run(STEPS[command])