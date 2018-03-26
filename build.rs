use std::env;
use std::fs;
use std::process::Command;

fn main() {
  let mut curr_dir = env::current_dir().expect("Get project directory");
  curr_dir.push("src");
  curr_dir.push("parquet_thrift");

  // Check if thrift is installed
  match run(Command::new("thrift").arg("--version")) {
    Ok(ref version_str) if check_thrift_version(version_str) => {
      println!("Version {}", version_str);
    },
    Ok(ref version_str) => {
      thrift_error(&format!("Invalid version: {}", version_str));
    },
    Err(error) => {
      thrift_error(&error);
    }
  }

  // List all thrift files in the directory
  let paths = fs::read_dir(curr_dir).expect("List files in target directory");

  for path in paths {
    if let Ok(ref dir_entry) = path {
      let file_name = dir_entry.file_name().into_string().unwrap();
      let file_path = dir_entry.path();

      if file_name.ends_with(".thrift") {
        let parent_dir = file_path.parent().unwrap();
        // Run command to generate thrift file
        run(
          Command::new("thrift")
            .args(&["--gen", "rs", &file_name])
            .current_dir(parent_dir)
        ).unwrap();
      }
    }
  }
}

/// Runs command and returns either content of stdout for successful execution, or
/// an error message otherwise.
fn run(command: &mut Command) -> Result<String, String> {
  println!("Running: `{:?}`", command);
  match command.output() {
    Ok(ref output) if output.status.success() => {
      Ok(String::from_utf8_lossy(&output.stdout).to_string())
    },
    Ok(ref output) => {
      Err(format!("Failed: `{:?}` ({})", command, output.status))
    },
    Err(error) => {
      Err(format!("Failed: `{:?}` ({})", command, error))
    }
  }
}

/// Panics and displays thrift error.
fn thrift_error(error: &str) {
  panic!("
    ========================
    Thrift is not installed!
    ========================

    Thrift with Rust support is required to build the project.
    See installation guide on https://thrift.apache.org website.
    Or follow the steps in 'before_script' section of '.travis.yml'.

    Error: {}
  ", error);
}

/// Checks thrift version and returns true if version is sufficient, and false otherwise.
fn check_thrift_version(version_str: &str) -> bool {
  // Version looks like "Thrift version X.Y.Z[-abc]"
  let parts: Vec<&str> = version_str.split_whitespace().collect();
  if parts.len() != 3 {
    return false;
  }

  // Remove `-dev` suffix
  let version = parts[2].split(|c| c == '-').next().unwrap_or("0.0.0");
  // Split version into major, minor and patch
  let semver: Vec<&str> = version.split(|c| c == '.').collect();
  let major = semver[0].parse::<usize>().unwrap_or(0);
  let minor = semver[1].parse::<usize>().unwrap_or(0);
  let _patch = semver[2].parse::<usize>().unwrap_or(0);

  major >= 1 || minor > 10
}
