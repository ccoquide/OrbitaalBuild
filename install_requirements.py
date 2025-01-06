import subprocess
import sys

# List of required packages with their versions
required_packages = [
    "pyspark==3.5.0",
    "numpy==1.24.4",
    "pandas==2.0.3",
    "pytz==2024.1"
]

def install_package(package):
    """Installs a Python package using pip."""
    try:
        # Try to install the package
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Successfully installed: {package}")
    except subprocess.CalledProcessError:
        print(f"Failed to install: {package}")

def main():
    print("Installing required packages...")
    for package in required_packages:
        install_package(package)

if __name__ == "__main__":
    main()
