//! Source-tree correlation metadata for performance history.

use std::{
    collections::BTreeMap,
    env,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Marker {
    pub schema: u32,
    pub history_id: String,
    pub history_id_override: Option<String>,
    pub history_description: String,
    pub profile: String,
    pub timestamp_utc: String,
    pub source: Source,
    pub tools: BTreeMap<String, String>,
    pub uname: String,
    pub invoked_args: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "vcs", rename_all = "lowercase")]
pub enum Source {
    Jj {
        change_id: String,
        commit_id: String,
        bookmarks: String,
        description: String,
        dirty: bool,
        tree_matches_parent: bool,
        parent_change_id: String,
        parent_commit_id: String,
    },
    Git {
        commit_id: String,
        bookmarks: String,
        description: String,
        dirty: bool,
        worktree_fingerprint: Option<String>,
    },
}

impl Marker {
    pub fn collect(profile: String, invoked_args: Vec<String>) -> Result<Self, String> {
        let source = collect_source()?;
        let default_id = match &source {
            Source::Jj {
                change_id,
                commit_id,
                ..
            } => format!("jj-{}-{}", short(change_id), short(commit_id)),
            Source::Git {
                commit_id,
                worktree_fingerprint,
                ..
            } => match worktree_fingerprint {
                Some(fingerprint) => {
                    format!("git-{}-dirty-{}", short(commit_id), short(fingerprint))
                }
                None => format!("git-{}", short(commit_id)),
            },
        };
        let override_id = env::var("HISTORY_ID").ok();
        let history_id = sanitize_history_id(override_id.as_deref().unwrap_or(&default_id));
        let source_summary = match &source {
            Source::Jj {
                change_id,
                commit_id,
                bookmarks,
                description,
                dirty,
                tree_matches_parent,
                ..
            } => format!(
                "jj change={change_id} commit={commit_id} bookmarks={bookmarks:?} dirty={dirty} tree_matches_parent={tree_matches_parent} description={description:?}"
            ),
            Source::Git {
                commit_id,
                bookmarks,
                description,
                dirty,
                worktree_fingerprint,
            } => format!(
                "git commit={commit_id} bookmarks={bookmarks:?} dirty={dirty} worktree_fingerprint={worktree_fingerprint:?} description={description:?}"
            ),
        };
        let default_description = format!("profile={profile}; {source_summary}");
        let history_description = env::var("HISTORY_DESCRIPTION").unwrap_or(default_description);
        let tools = ["rustc", "cargo", "cargo-criterion"]
            .into_iter()
            .map(|tool| Ok((tool.to_owned(), first_line(&run(tool, &["--version"])?))))
            .collect::<Result<_, String>>()?;
        let environment = [
            "CI",
            "CARGO_TARGET_DIR",
            "RUSTFLAGS",
            "RUSTUP_TOOLCHAIN",
            "CARGO_PROFILE_BENCH_LTO",
            "CARGO_PROFILE_BENCH_CODEGEN_UNITS",
        ]
        .into_iter()
        .filter_map(|key| env::var(key).ok().map(|value| (key.to_owned(), value)))
        .chain([("PW_CRITERION_PROFILE".to_owned(), profile.clone())])
        .collect();
        Ok(Self {
            schema: 1,
            history_id,
            history_id_override: override_id,
            history_description,
            profile,
            timestamp_utc: first_line(&run("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"])?),
            source,
            tools,
            uname: first_line(&run("uname", &["-a"])?),
            invoked_args,
            environment,
        })
    }
}

pub fn sanitize_history_id(value: &str) -> String {
    let mut result = String::with_capacity(value.len().min(80));
    let mut separator = false;
    for character in value.chars() {
        if result.len() >= 80 {
            break;
        }
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
            result.push(character);
            separator = false;
        } else if !separator && !result.is_empty() {
            result.push('-');
            separator = true;
        }
    }
    let trimmed = result.trim_matches('-').to_owned();
    if trimmed.is_empty() {
        "unnamed".to_owned()
    } else {
        trimmed
    }
}

fn collect_source() -> Result<Source, String> {
    if command_succeeds("jj", &["root"]) {
        let change_id = jj_field("@", "change_id")?;
        let commit_id = jj_field("@", "commit_id")?;
        let parent_change_id = jj_field("@-", "change_id")?;
        let parent_commit_id = jj_field("@-", "commit_id")?;
        let bookmarks = jj_field("@", "bookmarks")?;
        let description = jj_field("@", "description.first_line()")?;
        let summary = run("jj", &["diff", "--summary"])?;
        return Ok(Source::Jj {
            change_id,
            commit_id,
            bookmarks,
            description,
            dirty: !summary.trim().is_empty(),
            tree_matches_parent: summary.trim().is_empty(),
            parent_change_id,
            parent_commit_id,
        });
    }
    let commit_id = first_line(&run("git", &["rev-parse", "HEAD"])?);
    let status = run(
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
    )?;
    let dirty = !status.is_empty();
    Ok(Source::Git {
        bookmarks: first_line(&run("git", &["branch", "--show-current"])?),
        description: first_line(&run("git", &["log", "-1", "--pretty=%s"])?),
        worktree_fingerprint: dirty.then(worktree_fingerprint).transpose()?,
        commit_id,
        dirty,
    })
}

fn worktree_fingerprint() -> Result<String, String> {
    let mut material = run_bytes("git", &["diff", "--binary", "--no-ext-diff", "HEAD"])?;
    let untracked = run("git", &["ls-files", "--others", "--exclude-standard"])?;
    for path in untracked.lines() {
        material.extend_from_slice(path.as_bytes());
        material.push(0);
        material.extend(std::fs::read(Path::new(path)).map_err(|error| error.to_string())?);
        material.push(0);
    }
    let mut child = Command::new("git")
        .args(["hash-object", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to run git hash-object: {error}"))?;
    child
        .stdin
        .take()
        .ok_or("git hash-object stdin unavailable")?
        .write_all(&material)
        .map_err(|error| error.to_string())?;
    let output = child
        .wait_with_output()
        .map_err(|error| error.to_string())?;
    checked_output("git hash-object", output)
}

fn jj_field(revision: &str, template: &str) -> Result<String, String> {
    Ok(first_line(&run(
        "jj",
        &["log", "--no-graph", "-r", revision, "-T", template],
    )?))
}

fn short(value: &str) -> &str {
    &value[..value.len().min(12)]
}

fn first_line(value: &str) -> String {
    value.lines().next().unwrap_or_default().trim().to_owned()
}

fn command_succeeds(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn run(program: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|error| format!("failed to run {program}: {error}"))?;
    checked_output(program, output)
}

fn run_bytes(program: &str, args: &[&str]) -> Result<Vec<u8>, String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|error| format!("failed to run {program}: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn checked_output(program: &str, output: std::process::Output) -> Result<String, String> {
    if output.status.success() {
        String::from_utf8(output.stdout).map_err(|error| error.to_string())
    } else {
        Err(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_ids_are_filesystem_safe_bounded_and_nonempty() {
        assert_eq!(sanitize_history_id(" release/foo @ 1 "), "release-foo-1");
        assert_eq!(sanitize_history_id("///"), "unnamed");
        assert_eq!(sanitize_history_id(&"x".repeat(100)).len(), 80);
        assert!(sanitize_history_id("a:b\nc")
            .chars()
            .all(|c| { c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') }));
    }
}
