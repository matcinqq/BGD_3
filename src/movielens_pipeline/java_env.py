import os
import platform
import shutil
import subprocess
from pathlib import Path

"""
Java environment config module (for Spark).
Detects Java installations and sets JAVA_HOME for Spark to work.
"""

def _java_executable_for_home(java_home):
    bin_dir = Path(java_home) / "bin"
    candidates = [bin_dir / "java"]
    if platform.system() == "Windows":
        candidates.insert(0, bin_dir / "java.exe")

    for java_executable in candidates:
        if java_executable.exists():
            return java_executable
    return None


def _candidate_java_homes():
    system = platform.system()
    candidates = []

    if system == "Darwin":
        candidates.extend(
            [
                Path("/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
                Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
            ]
        )
        jvm_root = Path("/Library/Java/JavaVirtualMachines")
        if jvm_root.exists():
            for jdk_dir in sorted(jvm_root.glob("*")):
                candidates.append(jdk_dir / "Contents" / "Home")
    elif system == "Windows":
        base_dirs = []
        for env_name in ("ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"):
            env_value = os.environ.get(env_name)
            if env_value:
                base_dirs.append(Path(env_value))

        for base_dir in base_dirs:
            candidates.extend(base_dir.glob("Java/jdk*"))
            candidates.extend(base_dir.glob("Eclipse Adoptium/jdk*"))
            candidates.extend(base_dir.glob("Microsoft/jdk*"))
    else:
        jvm_root = Path("/usr/lib/jvm")
        if jvm_root.exists():
            for jdk_dir in sorted(jvm_root.glob("*")):
                candidates.append(jdk_dir)

    def preference(path_obj):
        path_str = str(path_obj).lower()
        return (0 if "21" in path_str else 1, path_str)

    return sorted(candidates, key=preference)


def _java_home_from_java_command(java_executable):
    try:
        result = subprocess.run(
            [java_executable, "-XshowSettings:properties", "-version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None

    output = f"{result.stdout}\n{result.stderr}"
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("java.home = "):
            home = stripped.split("=", 1)[1].strip()
            if home:
                return home
    return None


def configure_java_for_spark():
    current_java_home = os.environ.get("JAVA_HOME")
    if current_java_home and _java_executable_for_home(current_java_home):
        return current_java_home

    for home in _candidate_java_homes():
        if _java_executable_for_home(home):
            os.environ["JAVA_HOME"] = str(home)
            os.environ["PATH"] = f"{home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
            return str(home)

    java_from_path = shutil.which("java")
    if java_from_path:
        java_home_guess = _java_home_from_java_command(java_from_path)
        if java_home_guess and _java_executable_for_home(java_home_guess):
            os.environ["JAVA_HOME"] = str(java_home_guess)
            os.environ["PATH"] = f"{Path(java_home_guess) / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
            return str(java_home_guess)

    return None

