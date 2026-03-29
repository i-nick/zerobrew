use console::style;

use crate::ui::StdUi;
use crate::utils::normalize_formula_name;

pub async fn execute(
    installer: &mut zb_io::Installer,
    formulas: Vec<String>,
    casks_only: bool,
    ui: &mut StdUi,
) -> Result<(), zb_core::Error> {
    let (mut outdated, warnings, skipped) = if formulas.is_empty() {
        let (outdated, warnings) = installer.check_outdated().await?;
        let filtered = if casks_only {
            outdated
                .into_iter()
                .filter(|pkg| pkg.name.starts_with("cask:"))
                .collect()
        } else {
            outdated
        };
        (filtered, warnings, Vec::new())
    } else {
        collect_selected_outdated(installer, formulas).await?
    };

    for warning in warnings {
        ui.warn(warning).map_err(ui_error)?;
    }

    outdated.sort_by(|a, b| a.name.cmp(&b.name));

    if outdated.is_empty() {
        let message = if casks_only {
            "All casks are up to date."
        } else if skipped.is_empty() {
            "All packages are up to date."
        } else {
            "All selected packages are up to date."
        };
        ui.info(message).map_err(ui_error)?;
        return Ok(());
    }

    let package_names: Vec<String> = outdated.iter().map(|pkg| pkg.name.clone()).collect();
    ui.heading(format!(
        "Upgrading {}...",
        style(package_names.join(", ")).bold()
    ))
    .map_err(ui_error)?;

    let mut upgraded_count = 0usize;
    let mut errors = Vec::new();

    for pkg in &outdated {
        let label = upgrade_label(pkg);
        ui.step_start(label).map_err(ui_error)?;

        let link = installer.is_linked(&pkg.name)?;
        match installer
            .install(std::slice::from_ref(&pkg.name), link)
            .await
        {
            Ok(result) => {
                upgraded_count += result.installed;
                ui.step_ok().map_err(ui_error)?;
            }
            Err(err) => {
                ui.step_fail().map_err(ui_error)?;
                errors.push((pkg.name.clone(), err));
            }
        }
    }

    if !skipped.is_empty() {
        ui.note(format!("Already up to date: {}", skipped.join(", ")))
            .map_err(ui_error)?;
    }

    if errors.is_empty() {
        ui.blank_line().map_err(ui_error)?;
        ui.heading(format!(
            "Upgraded {} {}",
            style(upgraded_count).green().bold(),
            if upgraded_count == 1 {
                "package"
            } else {
                "packages"
            }
        ))
        .map_err(ui_error)?;
        return Ok(());
    }

    for (name, err) in &errors {
        ui.error(format!("Failed to upgrade {}: {}", style(name).bold(), err))
            .map_err(ui_error)?;
    }

    Err(errors.remove(0).1)
}

async fn collect_selected_outdated(
    installer: &zb_io::Installer,
    formulas: Vec<String>,
) -> Result<(Vec<zb_io::OutdatedPackage>, Vec<String>, Vec<String>), zb_core::Error> {
    let mut outdated = Vec::new();
    let mut skipped = Vec::new();

    for formula in formulas {
        let name = normalize_formula_name(&formula)?;
        match installer.is_outdated(&name).await? {
            Some(pkg) => outdated.push(pkg),
            None => skipped.push(name),
        }
    }

    Ok((outdated, Vec::new(), skipped))
}

fn upgrade_label(pkg: &zb_io::OutdatedPackage) -> String {
    if pkg.installed_version == pkg.current_version {
        format!(
            "{} {} {}",
            style(&pkg.name).bold(),
            style(&pkg.current_version).yellow(),
            style("(new build)").dim()
        )
    } else {
        format!(
            "{} {} {} {}",
            style(&pkg.name).bold(),
            style(&pkg.installed_version).red(),
            style("→").dim(),
            style(&pkg.current_version).green()
        )
    }
}

fn ui_error(err: std::io::Error) -> zb_core::Error {
    zb_core::Error::StoreCorruption {
        message: format!("failed to write CLI output: {err}"),
    }
}
