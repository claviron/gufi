use std::path::Path;

/// Fill management metadata before preparing the launch inherited by workers.
/// An explicit label wins; detection never imports Python or changes dispatch.
pub(crate) fn resolve(
    root: &Path,
    frameworks: &mut Option<Vec<String>>,
) {
    if frameworks.is_some() {
        return;
    }

    let path = root.join("pyproject.toml");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "framework metadata could not be read");
            return;
        }
    };
    match detect(&text) {
        Ok(detected) => {
            *frameworks =
                Some(detected.into_iter().map(str::to_owned).collect())
        }
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "framework metadata could not be parsed");
        }
    }
}

fn detect(text: &str) -> Result<Vec<&'static str>, toml_edit::TomlError> {
    let document = toml_edit::Document::parse(text)?;
    let project = document
        .get("project")
        .and_then(|project| project.get("dependencies"))
        .and_then(toml_edit::Item::as_array)
        .into_iter()
        .flat_map(|dependencies| dependencies.iter())
        .filter_map(toml_edit::Value::as_str);
    let poetry = document
        .get("tool")
        .and_then(|tool| tool.get("poetry"))
        .and_then(|poetry| poetry.get("dependencies"))
        .and_then(toml_edit::Item::as_table_like)
        .into_iter()
        .flat_map(|dependencies| dependencies.iter())
        .filter(|(_, dependency)| {
            dependency.get("optional").and_then(toml_edit::Item::as_bool)
                != Some(true)
        })
        .map(|(name, _)| name);

    // Only declared runtime dependencies count, not optional or dev groups.
    let mut labels = Vec::new();
    for label in project.chain(poetry).filter_map(known_framework) {
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    Ok(labels)
}

fn known_framework(requirement: &str) -> Option<&'static str> {
    // A PEP 508 distribution name ends before extras, constraints or markers.
    // Match the whole name so helpers such as fastapi-cli do not count.
    let name = requirement.trim_start();
    let end = name
        .find(|character: char| {
            !character.is_ascii_alphanumeric()
                && !matches!(character, '-' | '_' | '.')
        })
        .unwrap_or(name.len());
    match name[..end].to_ascii_lowercase().as_str() {
        "fastapi" => Some("FastAPI"),
        "django" => Some("Django"),
        "starlette" => Some("Starlette"),
        "flask" => Some("Flask"),
        "quart" => Some("Quart"),
        "sanic" => Some("Sanic"),
        "litestar" => Some("Litestar"),
        "falcon" => Some("Falcon"),
        _ => None,
    }
}

#[cfg(test)]
#[path = "../tests/framework.rs"]
mod tests;
