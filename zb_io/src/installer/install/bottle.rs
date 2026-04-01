use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(target_os = "macos")]
use std::process::Command;

use tracing::warn;
use zb_core::{Error, InstallMethod, formula_token};

use crate::cellar::link::Linker;
use crate::cellar::materialize::Cellar;
use crate::cellar::materialize::copy_dir_copy_only;
use crate::installer::cask::resolve_cask;
use crate::network::download::{DownloadProgressCallback, DownloadRequest, DownloadResult};
use crate::progress::InstallProgress;
use crate::storage::db::KegFileRecord;

#[cfg(target_os = "macos")]
use tempfile::TempDir;

use super::{Installer, MAX_CORRUPTION_RETRIES, PlannedInstall};

const CASK_METADATA_FILE: &str = ".zerobrew-cask.json";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub(super) struct InstalledCaskMetadata {
    pub zap_paths: Vec<String>,
}

impl Installer {
    pub(super) async fn process_bottle_item(
        &mut self,
        item: &PlannedInstall,
        download: &DownloadResult,
        download_progress: &Option<DownloadProgressCallback>,
        link: bool,
        report: &impl Fn(InstallProgress),
    ) -> Result<(), Error> {
        let InstallMethod::Bottle(ref bottle) = item.method else {
            unreachable!()
        };
        let install_name = &item.install_name;
        let formula_name = &item.formula.name;
        let version = item.formula.effective_version();
        let store_key = &bottle.sha256;

        report(InstallProgress::UnpackStarted {
            name: formula_name.clone(),
        });

        let store_entry = self
            .extract_with_retry(download, &item.formula, bottle, download_progress.clone())
            .await?;

        let keg_path = self
            .cellar
            .materialize(formula_name, &version, &store_entry)?;

        report(InstallProgress::UnpackCompleted {
            name: formula_name.clone(),
        });

        let tx = self.db.transaction().inspect_err(|_| {
            Self::cleanup_materialized(&self.cellar, formula_name, &version);
        })?;

        tx.record_install(install_name, &version, store_key)
            .inspect_err(|_| {
                Self::cleanup_materialized(&self.cellar, formula_name, &version);
            })?;

        tx.commit().inspect_err(|_| {
            Self::cleanup_materialized(&self.cellar, formula_name, &version);
        })?;

        if let Err(e) = self.linker.link_opt(&keg_path) {
            warn!(formula = %install_name, error = %e, "failed to create opt link");
        }

        if link && !item.formula.is_keg_only() {
            report(InstallProgress::LinkStarted {
                name: formula_name.clone(),
            });
            match self.linker.link_keg(&keg_path) {
                Ok(linked_files) => {
                    report(InstallProgress::LinkCompleted {
                        name: formula_name.clone(),
                    });
                    self.record_linked_files(install_name, &version, &linked_files);
                }
                Err(e) => {
                    let _ = self.linker.unlink_keg(&keg_path);
                    report(InstallProgress::InstallCompleted {
                        name: formula_name.clone(),
                    });
                    return Err(e);
                }
            }
        } else if link && item.formula.is_keg_only() {
            let reason = match &item.formula.keg_only {
                zb_core::KegOnly::Reason(s) => s.clone(),
                _ if formula_name.contains('@') => "versioned formula".to_string(),
                _ => "keg-only formula".to_string(),
            };
            report(InstallProgress::LinkSkipped {
                name: formula_name.clone(),
                reason,
            });
        }

        report(InstallProgress::InstallCompleted {
            name: formula_name.clone(),
        });

        Ok(())
    }

    async fn extract_with_retry(
        &self,
        download: &DownloadResult,
        formula: &zb_core::Formula,
        bottle: &zb_core::SelectedBottle,
        progress: Option<DownloadProgressCallback>,
    ) -> Result<std::path::PathBuf, Error> {
        let mut blob_path = download.blob_path.clone();
        let mut last_error = None;

        for attempt in 0..MAX_CORRUPTION_RETRIES {
            match self.store.ensure_entry(&bottle.sha256, &blob_path) {
                Ok(entry) => return Ok(entry),
                Err(Error::StoreCorruption { message }) => {
                    self.downloader.remove_blob(&bottle.sha256);

                    if attempt + 1 < MAX_CORRUPTION_RETRIES {
                        warn!(
                            formula = %formula.name,
                            attempt = attempt + 2,
                            max_retries = MAX_CORRUPTION_RETRIES,
                            "corrupted download detected; retrying"
                        );

                        let request = DownloadRequest {
                            url: bottle.url.clone(),
                            sha256: bottle.sha256.clone(),
                            name: formula.name.clone(),
                        };

                        match self
                            .downloader
                            .download_single(request, progress.clone())
                            .await
                        {
                            Ok(new_path) => {
                                blob_path = new_path;
                            }
                            Err(e) => {
                                last_error = Some(e);
                                break;
                            }
                        }
                    } else {
                        last_error = Some(Error::StoreCorruption {
                            message: format!(
                                "{message}\n\nFailed after {MAX_CORRUPTION_RETRIES} attempts. The download may be corrupted at the source."
                            ),
                        });
                    }
                }
                Err(e) => {
                    last_error = Some(e);
                    break;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::StoreCorruption {
            message: "extraction failed with unknown error".to_string(),
        }))
    }

    fn record_linked_files(
        &mut self,
        name: &str,
        version: &str,
        linked_files: &[crate::cellar::link::LinkedFile],
    ) {
        if let Ok(tx) = self.db.transaction() {
            let mut ok = true;
            for linked in linked_files {
                if tx
                    .record_linked_file(
                        name,
                        version,
                        &linked.link_path.to_string_lossy(),
                        &linked.target_path.to_string_lossy(),
                    )
                    .is_err()
                {
                    ok = false;
                    break;
                }
            }
            if ok {
                let _ = tx.commit();
            }
        }
    }

    pub(super) fn cleanup_failed_install(
        linker: &Linker,
        cellar: &Cellar,
        name: &str,
        version: &str,
        keg_path: &Path,
        unlink: bool,
    ) {
        if unlink && let Err(e) = linker.unlink_keg(keg_path) {
            warn!(
                formula = %name,
                version = %version,
                error = %e,
                "failed to clean up links after install error"
            );
        }

        if let Err(e) = cellar.remove_keg(name, version) {
            warn!(
                formula = %name,
                version = %version,
                error = %e,
                "failed to remove keg after install error"
            );
        }
    }

    pub(super) async fn install_single_cask(
        &mut self,
        token: &str,
        link: bool,
    ) -> Result<(), Error> {
        let cask_json = self.api_client.get_cask(token).await?;
        let cask = resolve_cask(token, &cask_json)?;
        let previous_install = self.db.get_installed(&cask.install_name);
        let previous_records = self.db.list_keg_files_for_name(&cask.install_name)?;
        let previous_external_paths = external_artifact_paths(&previous_records, &self.prefix);

        let blob_path = self
            .downloader
            .download_single(
                DownloadRequest {
                    url: cask.url.clone(),
                    sha256: cask.sha256.clone(),
                    name: cask.install_name.clone(),
                },
                None,
            )
            .await?;

        if let Some(previous) = previous_install.as_ref() {
            let previous_keg_path = self.cellar.keg_path(&previous.name, &previous.version);
            let _ = self.linker.unlink_keg(&previous_keg_path);

            if previous.version == cask.version {
                self.cellar.remove_keg(&previous.name, &previous.version)?;
            }
        }

        let keg_path = self.cellar.keg_path(&cask.install_name, &cask.version);
        let mut cleanup = FailedInstallGuard::new(
            &self.linker,
            &self.cellar,
            &cask.install_name,
            &cask.version,
            &keg_path,
            link,
        );

        #[cfg(target_os = "macos")]
        if has_app_backed_payload(&cask) {
            verify_app_conflicts(&self.db, &cask, &previous_external_paths)?;
        }

        if should_mount_dmg_directly(&cask) {
            #[cfg(target_os = "macos")]
            {
                let mounted = MountedDmg::attach(&blob_path, &cask)?;
                install_cask_from_root(
                    mounted.path(),
                    &keg_path,
                    &cask,
                    &previous_external_paths,
                    &mut cleanup,
                )?;
            }
            #[cfg(not(target_os = "macos"))]
            {
                return Err(Error::InvalidArgument {
                    message: format!(
                        "cask '{}' installs app bundles, which are only supported on macOS",
                        cask.token
                    ),
                });
            }
        } else if crate::extraction::is_archive(&blob_path)? {
            let extracted = self.store.ensure_entry(&cask.sha256, &blob_path)?;
            install_cask_from_root(
                &extracted,
                &keg_path,
                &cask,
                &previous_external_paths,
                &mut cleanup,
            )?;
        } else if has_app_backed_payload(&cask) {
            #[cfg(target_os = "macos")]
            {
                let mounted = MountedDmg::attach(&blob_path, &cask)?;
                install_cask_from_root(
                    mounted.path(),
                    &keg_path,
                    &cask,
                    &previous_external_paths,
                    &mut cleanup,
                )?;
            }
            #[cfg(not(target_os = "macos"))]
            {
                return Err(Error::InvalidArgument {
                    message: format!(
                        "cask '{}' installs app bundles, which are only supported on macOS",
                        cask.token
                    ),
                });
            }
        } else {
            stage_raw_cask_binary(&blob_path, &keg_path, &cask)?;
        }

        write_cask_metadata(
            &keg_path,
            &InstalledCaskMetadata {
                zap_paths: cask.zap.paths.clone(),
            },
        )?;

        let linked_files = if link {
            self.linker.link_keg(&keg_path)?
        } else {
            Vec::new()
        };

        let tx = self.db.transaction()?;
        tx.record_install(&cask.install_name, &cask.version, &cask.sha256)?;
        tx.clear_keg_file_records(&cask.install_name)?;
        for linked in &linked_files {
            tx.record_linked_file(
                &cask.install_name,
                &cask.version,
                &linked.link_path.to_string_lossy(),
                &linked.target_path.to_string_lossy(),
            )?;
        }
        for external in cleanup.external_records() {
            tx.record_linked_file(
                &cask.install_name,
                &cask.version,
                &external.link_path.to_string_lossy(),
                &external.target_path.to_string_lossy(),
            )?;
        }
        tx.commit()?;

        cleanup.disarm();

        if let Some(previous) = previous_install
            && previous.version != cask.version
        {
            self.cellar.remove_keg(&previous.name, &previous.version)?;
        }

        Ok(())
    }
}

fn should_mount_dmg_directly(cask: &crate::installer::cask::ResolvedCask) -> bool {
    has_app_backed_payload(cask)
        && cask
            .url
            .split('?')
            .next()
            .is_some_and(|url| url.to_ascii_lowercase().ends_with(".dmg"))
}

pub(super) fn read_cask_metadata(keg_path: &Path) -> Result<InstalledCaskMetadata, Error> {
    let metadata_path = keg_path.join(CASK_METADATA_FILE);
    match fs::read(&metadata_path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(Error::store("failed to parse installed cask metadata")),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(InstalledCaskMetadata::default())
        }
        Err(err) => Err(Error::store("failed to read installed cask metadata")(err)),
    }
}

fn write_cask_metadata(keg_path: &Path, metadata: &InstalledCaskMetadata) -> Result<(), Error> {
    let metadata_path = keg_path.join(CASK_METADATA_FILE);
    let bytes =
        serde_json::to_vec(metadata).map_err(Error::store("failed to serialize cask metadata"))?;
    fs::write(metadata_path, bytes).map_err(Error::store("failed to write cask metadata"))
}

pub(super) fn dependency_cellar_path(
    cellar: &Cellar,
    installed_name: &str,
    version: &str,
) -> String {
    cellar
        .keg_path(formula_token(installed_name), version)
        .display()
        .to_string()
}

struct FailedInstallGuard<'a> {
    linker: &'a Linker,
    cellar: &'a Cellar,
    name: &'a str,
    version: &'a str,
    keg_path: &'a Path,
    unlink: bool,
    armed: bool,
    external_paths: Vec<PathBuf>,
    external_records: Vec<crate::cellar::link::LinkedFile>,
}

impl<'a> FailedInstallGuard<'a> {
    fn new(
        linker: &'a Linker,
        cellar: &'a Cellar,
        name: &'a str,
        version: &'a str,
        keg_path: &'a Path,
        unlink: bool,
    ) -> Self {
        Self {
            linker,
            cellar,
            name,
            version,
            keg_path,
            unlink,
            armed: true,
            external_paths: Vec::new(),
            external_records: Vec::new(),
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn track_external_path(&mut self, path: PathBuf) {
        self.external_records.push(crate::cellar::link::LinkedFile {
            link_path: path.clone(),
            target_path: path.clone(),
        });
        self.external_paths.push(path);
    }

    fn external_records(&self) -> &[crate::cellar::link::LinkedFile] {
        &self.external_records
    }
}

impl Drop for FailedInstallGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            for path in self.external_paths.iter().rev() {
                let _ = remove_path(path);
            }
            Installer::cleanup_failed_install(
                self.linker,
                self.cellar,
                self.name,
                self.version,
                self.keg_path,
                self.unlink,
            );
        }
    }
}

fn has_app_backed_payload(cask: &crate::installer::cask::ResolvedCask) -> bool {
    !cask.apps.is_empty()
        || cask
            .binaries
            .iter()
            .any(|binary| binary.source.starts_with("$APPDIR/"))
}

fn external_artifact_paths(records: &[KegFileRecord], prefix: &Path) -> HashSet<PathBuf> {
    records
        .iter()
        .map(|record| PathBuf::from(&record.linked_path))
        .filter(|path| !path.starts_with(prefix))
        .collect()
}

#[cfg(target_os = "macos")]
fn verify_app_conflicts(
    db: &crate::storage::db::Database,
    cask: &crate::installer::cask::ResolvedCask,
    previous_external_paths: &HashSet<PathBuf>,
) -> Result<(), Error> {
    let app_dir = user_applications_dir()?;
    for app in &cask.apps {
        let destination = app_dir.join(&app.target);
        if destination.symlink_metadata().is_ok() && !previous_external_paths.contains(&destination)
        {
            return Err(Error::LinkConflict {
                conflicts: vec![zb_core::ConflictedLink {
                    path: destination.clone(),
                    owned_by: db.find_keg_file_owner(destination.to_string_lossy().as_ref())?,
                }],
            });
        }
    }
    Ok(())
}

fn install_cask_from_root(
    source_root: &Path,
    keg_path: &Path,
    cask: &crate::installer::cask::ResolvedCask,
    previous_external_paths: &HashSet<PathBuf>,
    cleanup: &mut FailedInstallGuard<'_>,
) -> Result<(), Error> {
    let installed_apps = if has_app_backed_payload(cask) {
        #[cfg(target_os = "macos")]
        {
            install_cask_apps(source_root, cask, previous_external_paths, cleanup)?
        }
        #[cfg(not(target_os = "macos"))]
        {
            return Err(Error::InvalidArgument {
                message: format!(
                    "cask '{}' installs app bundles, which are only supported on macOS",
                    cask.token
                ),
            });
        }
    } else {
        HashMap::new()
    };

    stage_cask_binaries(source_root, keg_path, cask, &installed_apps)
}

#[cfg(target_os = "macos")]
fn install_cask_apps(
    source_root: &Path,
    cask: &crate::installer::cask::ResolvedCask,
    previous_external_paths: &HashSet<PathBuf>,
    cleanup: &mut FailedInstallGuard<'_>,
) -> Result<HashMap<String, PathBuf>, Error> {
    let app_dir = user_applications_dir()?;
    fs::create_dir_all(&app_dir).map_err(Error::store("failed to create user Applications dir"))?;

    let mut installed_apps = HashMap::new();
    let desired_destinations: HashSet<PathBuf> = cask
        .apps
        .iter()
        .map(|app| app_dir.join(&app.target))
        .collect();

    for app in &cask.apps {
        let source = resolve_relative_cask_source_path(source_root, cask, &app.source, "app")?;
        if !source.exists() {
            return Err(Error::InvalidArgument {
                message: format!(
                    "cask '{}' app source '{}' not found in container",
                    cask.token, app.source
                ),
            });
        }

        let destination = app_dir.join(&app.target);
        install_app_bundle(&source, &destination)?;
        cleanup.track_external_path(destination.clone());

        let source_name = file_name(&app.source)?;
        installed_apps.insert(source_name, destination.clone());
        installed_apps.insert(app.target.clone(), destination);
    }

    for stale_path in previous_external_paths {
        if !desired_destinations.contains(stale_path) {
            remove_path(stale_path)?;
        }
    }

    Ok(installed_apps)
}

#[cfg(target_os = "macos")]
fn install_app_bundle(source: &Path, destination: &Path) -> Result<(), Error> {
    if !source.is_dir() {
        return Err(Error::InvalidArgument {
            message: format!(
                "cask app source '{}' is not an app bundle directory",
                source.display()
            ),
        });
    }

    let parent = destination.parent().ok_or_else(|| Error::StoreCorruption {
        message: format!(
            "invalid app install path '{}': missing parent directory",
            destination.display()
        ),
    })?;
    fs::create_dir_all(parent).map_err(Error::store("failed to create app destination parent"))?;

    let staging_dir = tempfile::Builder::new()
        .prefix(".zerobrew-app-")
        .tempdir_in(parent)
        .map_err(Error::store("failed to create app staging directory"))?;
    let staged_bundle =
        staging_dir.path().join(
            destination
                .file_name()
                .ok_or_else(|| Error::InvalidArgument {
                    message: format!("invalid app install path '{}'", destination.display()),
                })?,
        );

    copy_dir_copy_only(source, &staged_bundle)?;
    remove_path(destination)?;
    fs::rename(&staged_bundle, destination)
        .map_err(Error::store("failed to install app bundle"))?;

    Ok(())
}

fn stage_cask_binaries(
    source_root: &Path,
    keg_path: &Path,
    cask: &crate::installer::cask::ResolvedCask,
    installed_apps: &HashMap<String, PathBuf>,
) -> Result<(), Error> {
    let bin_dir = keg_path.join("bin");
    fs::create_dir_all(&bin_dir).map_err(Error::store("failed to create cask bin dir"))?;

    for binary in &cask.binaries {
        let source =
            resolve_cask_binary_source_path(source_root, cask, &binary.source, installed_apps)?;
        if !source.exists() {
            if cask.has_preflight {
                return Err(Error::InvalidArgument {
                    message: format!(
                        "cask '{}' uses preflight-generated wrapper artifacts which are not supported yet",
                        cask.token
                    ),
                });
            }
            return Err(Error::InvalidArgument {
                message: format!(
                    "cask '{}' binary source '{}' not found in container",
                    cask.token, binary.source
                ),
            });
        }
        if source.is_dir() {
            return Err(Error::InvalidArgument {
                message: format!(
                    "cask '{}' binary source '{}' resolved to a directory",
                    cask.token, binary.source
                ),
            });
        }

        let target = bin_dir.join(&binary.target);
        if target.symlink_metadata().is_ok() {
            remove_path(&target)?;
        }

        if binary.source.starts_with("$APPDIR/") {
            #[cfg(unix)]
            std::os::unix::fs::symlink(&source, &target)
                .map_err(Error::store("failed to stage cask app binary symlink"))?;

            #[cfg(not(unix))]
            fs::copy(&source, &target).map_err(|e| Error::StoreCorruption {
                message: format!("failed to stage cask binary '{}': {e}", binary.target),
            })?;
        } else {
            fs::copy(&source, &target).map_err(|e| Error::StoreCorruption {
                message: format!("failed to stage cask binary '{}': {e}", binary.target),
            })?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = fs::metadata(&target)
                    .map_err(Error::store("failed to read staged cask binary metadata"))?
                    .permissions();
                if perms.mode() & 0o111 == 0 {
                    perms.set_mode(0o755);
                    fs::set_permissions(&target, perms)
                        .map_err(Error::store("failed to make staged cask binary executable"))?;
                }
            }
        }
    }

    Ok(())
}

fn stage_raw_cask_binary(
    blob_path: &Path,
    keg_path: &Path,
    cask: &crate::installer::cask::ResolvedCask,
) -> Result<(), Error> {
    if cask.binaries.len() != 1 {
        return Err(Error::InvalidArgument {
            message: format!(
                "cask '{}' has {} binary artifacts but the download is a raw binary; expected exactly 1",
                cask.token,
                cask.binaries.len()
            ),
        });
    }

    let binary = &cask.binaries[0];
    let bin_dir = keg_path.join("bin");
    fs::create_dir_all(&bin_dir).map_err(Error::store("failed to create cask bin dir"))?;

    let target = bin_dir.join(&binary.target);
    if target.symlink_metadata().is_ok() {
        remove_path(&target)?;
    }

    fs::copy(blob_path, &target).map_err(|e| Error::StoreCorruption {
        message: format!("failed to stage cask binary '{}': {e}", binary.target),
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755))
            .map_err(Error::store("failed to make staged cask binary executable"))?;
    }

    Ok(())
}

fn resolve_cask_binary_source_path(
    source_root: &Path,
    cask: &crate::installer::cask::ResolvedCask,
    source: &str,
    installed_apps: &HashMap<String, PathBuf>,
) -> Result<PathBuf, Error> {
    if source.starts_with("$APPDIR/") {
        return resolve_appdir_binary_source_path(cask, source, installed_apps);
    }

    resolve_relative_cask_source_path(source_root, cask, source, "binary")
}

fn resolve_relative_cask_source_path(
    source_root: &Path,
    cask: &crate::installer::cask::ResolvedCask,
    source: &str,
    artifact_type: &str,
) -> Result<PathBuf, Error> {
    let mut normalized = source.to_string();
    let caskroom_prefix = format!("$HOMEBREW_PREFIX/Caskroom/{}/{}/", cask.token, cask.version);
    if let Some(stripped) = normalized.strip_prefix(&caskroom_prefix) {
        normalized = stripped.to_string();
    }

    let source_path = Path::new(&normalized);
    if source_path.is_absolute() {
        return Err(Error::InvalidArgument {
            message: format!(
                "cask '{}' {artifact_type} source '{}' must be a relative path",
                cask.token, source,
            ),
        });
    }

    for component in source_path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(Error::InvalidArgument {
                message: format!(
                    "cask '{}' {artifact_type} source '{}' cannot contain '..'",
                    cask.token, source,
                ),
            });
        }
    }

    Ok(source_root.join(source_path))
}

fn resolve_appdir_binary_source_path(
    cask: &crate::installer::cask::ResolvedCask,
    source: &str,
    installed_apps: &HashMap<String, PathBuf>,
) -> Result<PathBuf, Error> {
    let stripped = source
        .strip_prefix("$APPDIR/")
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "cask '{}' binary source '{}' is not a valid APPDIR path",
                cask.token, source
            ),
        })?;
    let relative_path = Path::new(stripped);
    let mut components = relative_path.components();
    let app_component = components
        .next()
        .and_then(|component| match component {
            std::path::Component::Normal(name) => name.to_str(),
            _ => None,
        })
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "cask '{}' binary source '{}' does not reference an installed app bundle",
                cask.token, source
            ),
        })?;

    let app_root = installed_apps
        .get(app_component)
        .ok_or_else(|| Error::InvalidArgument {
            message: format!(
                "cask '{}' binary source '{}' refers to unknown app '{}'",
                cask.token, source, app_component
            ),
        })?;

    let remainder = components.as_path();
    if remainder.as_os_str().is_empty() {
        Ok(app_root.clone())
    } else {
        Ok(app_root.join(remainder))
    }
}

fn file_name(path: &str) -> Result<String, Error> {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToString::to_string)
        .ok_or_else(|| Error::InvalidArgument {
            message: format!("invalid cask path '{path}'"),
        })
}

fn remove_path(path: &Path) -> Result<(), Error> {
    let metadata = match path.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(Error::store("failed to read path metadata")(err)),
    };

    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path).map_err(Error::store("failed to remove directory"))?;
    } else {
        fs::remove_file(path).map_err(Error::store("failed to remove file"))?;
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn user_applications_dir() -> Result<PathBuf, Error> {
    let home = std::env::var_os("HOME").ok_or_else(|| Error::InvalidArgument {
        message: "HOME must be set to install app-backed casks".to_string(),
    })?;
    Ok(PathBuf::from(home).join("Applications"))
}

#[cfg(target_os = "macos")]
struct MountedDmg {
    mount_dir: TempDir,
    attached: bool,
}

#[cfg(target_os = "macos")]
impl MountedDmg {
    fn attach(
        blob_path: &Path,
        cask: &crate::installer::cask::ResolvedCask,
    ) -> Result<Self, Error> {
        let mount_dir = tempfile::Builder::new()
            .prefix("zerobrew-dmg-")
            .tempdir()
            .map_err(Error::store("failed to create temporary DMG mountpoint"))?;

        let status = Command::new("/usr/bin/hdiutil")
            .arg("attach")
            .arg("-nobrowse")
            .arg("-readonly")
            .arg("-quiet")
            .arg("-mountpoint")
            .arg(mount_dir.path())
            .arg(blob_path)
            .status()
            .map_err(Error::exec("failed to execute hdiutil attach"))?;

        if !status.success() {
            return Err(Error::InvalidArgument {
                message: format!(
                    "cask '{}' download could not be mounted as a DMG",
                    cask.token
                ),
            });
        }

        Ok(Self {
            mount_dir,
            attached: true,
        })
    }

    fn path(&self) -> &Path {
        self.mount_dir.path()
    }
}

#[cfg(target_os = "macos")]
impl Drop for MountedDmg {
    fn drop(&mut self) {
        if !self.attached {
            return;
        }

        let status = Command::new("/usr/bin/hdiutil")
            .arg("detach")
            .arg("-quiet")
            .arg(self.mount_dir.path())
            .status();

        if !matches!(status, Ok(status) if status.success()) {
            let _ = Command::new("/usr/bin/hdiutil")
                .arg("detach")
                .arg("-force")
                .arg("-quiet")
                .arg(self.mount_dir.path())
                .status();
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "macos")]
    use std::ffi::OsString;
    use std::fs;
    #[cfg(target_os = "macos")]
    use std::sync::{LazyLock, Mutex};

    use tempfile::TempDir;
    #[cfg(target_os = "macos")]
    use wiremock::matchers::{method, path};
    #[cfg(target_os = "macos")]
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::cellar::Cellar;
    #[cfg(target_os = "macos")]
    use crate::installer::install::test_support::{create_cask_dmg, sha256_hex};
    #[cfg(target_os = "macos")]
    use crate::network::api::ApiClient;
    #[cfg(target_os = "macos")]
    use crate::storage::blob::BlobCache;
    use crate::storage::db::Database;
    #[cfg(target_os = "macos")]
    use crate::storage::store::Store;
    #[cfg(target_os = "macos")]
    use crate::{Installer, Linker};

    use super::*;

    #[cfg(target_os = "macos")]
    static HOME_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[cfg(target_os = "macos")]
    struct HomeOverride {
        previous: Option<OsString>,
    }

    #[cfg(target_os = "macos")]
    impl HomeOverride {
        fn set(path: &Path) -> Self {
            let previous = std::env::var_os("HOME");
            unsafe {
                std::env::set_var("HOME", path);
            }
            Self { previous }
        }
    }

    #[cfg(target_os = "macos")]
    impl Drop for HomeOverride {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = self.previous.take() {
                    std::env::set_var("HOME", previous);
                } else {
                    std::env::remove_var("HOME");
                }
            }
        }
    }

    fn empty_zap() -> crate::installer::cask::CaskZap {
        crate::installer::cask::CaskZap { paths: Vec::new() }
    }

    fn app_only_cask(url: &str, token: &str) -> crate::installer::cask::ResolvedCask {
        crate::installer::cask::ResolvedCask {
            install_name: format!("cask:{token}"),
            token: token.to_string(),
            version: "1.0.0".to_string(),
            url: url.to_string(),
            sha256: "aaa".to_string(),
            binaries: Vec::new(),
            apps: vec![crate::installer::cask::CaskApp {
                source: format!("{}.app", token),
                target: format!("{}.app", token),
            }],
            zap: empty_zap(),
            has_pkg: false,
            has_preflight: false,
        }
    }

    #[test]
    fn dependency_cellar_path_uses_formula_token_for_tap_name() {
        let tmp = TempDir::new().unwrap();
        let cellar = Cellar::new(tmp.path()).unwrap();
        let path = dependency_cellar_path(&cellar, "hashicorp/tap/terraform", "1.10.0");

        assert!(path.ends_with("cellar/terraform/1.10.0"));
    }

    #[test]
    fn dependency_cellar_path_keeps_core_formula_name() {
        let tmp = TempDir::new().unwrap();
        let cellar = Cellar::new(tmp.path()).unwrap();
        let path = dependency_cellar_path(&cellar, "openssl@3", "3.3.2");

        assert!(path.ends_with("cellar/openssl@3/3.3.2"));
    }

    #[test]
    fn dependency_cellar_path_uses_name_from_db_record() {
        let tmp = TempDir::new().unwrap();
        let cellar = Cellar::new(tmp.path()).unwrap();

        let db_path = tmp.path().join("zb.sqlite3");
        let mut db = Database::open(&db_path).unwrap();
        let tx = db.transaction().unwrap();
        tx.record_install("hashicorp/tap/terraform", "1.10.0", "store-key")
            .unwrap();
        tx.commit().unwrap();

        let keg = db.get_installed("hashicorp/tap/terraform").unwrap();
        let path = dependency_cellar_path(&cellar, &keg.name, &keg.version);

        assert!(path.ends_with("cellar/terraform/1.10.0"));
    }

    #[test]
    fn should_mount_dmg_directly_for_app_backed_dmg_urls() {
        let brave = app_only_cask("https://example.com/brave-browser.dmg", "Brave Browser");
        assert!(should_mount_dmg_directly(&brave));

        let zipped = app_only_cask("https://example.com/ghostty.zip", "Ghostty");
        assert!(!should_mount_dmg_directly(&zipped));
    }

    #[test]
    fn stage_raw_cask_binary_copies_and_marks_executable() {
        let tmp = TempDir::new().unwrap();
        let blob_path = tmp.path().join("claude");
        fs::write(&blob_path, b"#!/bin/sh\necho hello").unwrap();

        let keg_path = tmp.path().join("keg");
        let cask = crate::installer::cask::ResolvedCask {
            install_name: "cask:claude-code".to_string(),
            token: "claude-code".to_string(),
            version: "1.0.0".to_string(),
            url: "https://example.com/claude".to_string(),
            sha256: "aaa".to_string(),
            binaries: vec![crate::installer::cask::CaskBinary {
                source: "claude".to_string(),
                target: "claude".to_string(),
            }],
            apps: Vec::new(),
            zap: empty_zap(),
            has_pkg: false,
            has_preflight: false,
        };

        stage_raw_cask_binary(&blob_path, &keg_path, &cask).unwrap();

        let target = keg_path.join("bin/claude");
        assert!(target.exists());
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            "#!/bin/sh\necho hello"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&target).unwrap().permissions().mode();
            assert_eq!(mode & 0o755, 0o755);
        }
    }

    #[test]
    fn stage_raw_cask_binary_rejects_multiple_binaries() {
        let tmp = TempDir::new().unwrap();
        let blob_path = tmp.path().join("blob");
        fs::write(&blob_path, b"data").unwrap();

        let keg_path = tmp.path().join("keg");
        let cask = crate::installer::cask::ResolvedCask {
            install_name: "cask:multi".to_string(),
            token: "multi".to_string(),
            version: "1.0.0".to_string(),
            url: "https://example.com/multi".to_string(),
            sha256: "bbb".to_string(),
            binaries: vec![
                crate::installer::cask::CaskBinary {
                    source: "a".to_string(),
                    target: "a".to_string(),
                },
                crate::installer::cask::CaskBinary {
                    source: "b".to_string(),
                    target: "b".to_string(),
                },
            ],
            apps: Vec::new(),
            zap: empty_zap(),
            has_pkg: false,
            has_preflight: false,
        };

        let err = stage_raw_cask_binary(&blob_path, &keg_path, &cask).unwrap_err();
        assert!(err.to_string().contains("raw binary"));
    }

    #[test]
    fn resolve_appdir_binary_source_path_uses_installed_app_path() {
        let mut installed_apps = HashMap::new();
        installed_apps.insert(
            "Zed.app".to_string(),
            PathBuf::from("/Users/test/Applications/Zed.app"),
        );

        let cask = crate::installer::cask::ResolvedCask {
            install_name: "cask:zed".to_string(),
            token: "zed".to_string(),
            version: "1.0.0".to_string(),
            url: "https://example.com/zed.dmg".to_string(),
            sha256: "aaa".to_string(),
            binaries: vec![crate::installer::cask::CaskBinary {
                source: "$APPDIR/Zed.app/Contents/MacOS/cli".to_string(),
                target: "zed".to_string(),
            }],
            apps: vec![crate::installer::cask::CaskApp {
                source: "Zed.app".to_string(),
                target: "Zed.app".to_string(),
            }],
            zap: empty_zap(),
            has_pkg: false,
            has_preflight: false,
        };

        let resolved = resolve_appdir_binary_source_path(
            &cask,
            "$APPDIR/Zed.app/Contents/MacOS/cli",
            &installed_apps,
        )
        .unwrap();
        assert_eq!(
            resolved,
            PathBuf::from("/Users/test/Applications/Zed.app/Contents/MacOS/cli")
        );
    }

    #[test]
    fn stage_cask_binaries_reports_preflight_wrapper_as_unsupported() {
        let tmp = TempDir::new().unwrap();
        let source_root = tmp.path().join("container");
        fs::create_dir_all(&source_root).unwrap();
        let keg_path = tmp.path().join("keg");

        let cask = crate::installer::cask::ResolvedCask {
            install_name: "cask:thorium".to_string(),
            token: "thorium".to_string(),
            version: "1.0.0".to_string(),
            url: "https://example.com/thorium.zip".to_string(),
            sha256: "aaa".to_string(),
            binaries: vec![crate::installer::cask::CaskBinary {
                source: "$HOMEBREW_PREFIX/Caskroom/thorium/1.0.0/thorium.wrapper.sh".to_string(),
                target: "thorium".to_string(),
            }],
            apps: vec![crate::installer::cask::CaskApp {
                source: "Thorium.app".to_string(),
                target: "Thorium.app".to_string(),
            }],
            zap: empty_zap(),
            has_pkg: false,
            has_preflight: true,
        };

        let err = stage_cask_binaries(&source_root, &keg_path, &cask, &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("preflight-generated wrapper"));
    }

    #[cfg(target_os = "macos")]
    fn cask_json(mock_server_uri: &str, version: &str, sha256: &str) -> String {
        format!(
            r#"{{
                "token": "zed",
                "version": "{version}",
                "url": "{mock_server_uri}/downloads/zed-{version}.dmg",
                "sha256": "{sha256}",
                "artifacts": [
                    {{ "app": ["Zed.app"] }},
                    {{ "binary": ["$APPDIR/Zed.app/Contents/MacOS/cli", {{ "target": "zed" }}] }}
                ]
            }}"#
        )
    }

    #[cfg(target_os = "macos")]
    fn app_only_cask_json(
        mock_server_uri: &str,
        token: &str,
        app_name: &str,
        version: &str,
        sha256: &str,
    ) -> String {
        serde_json::json!({
            "token": token,
            "version": version,
            "url": format!("{mock_server_uri}/{token}-{version}.dmg"),
            "sha256": sha256,
            "artifacts": [
                { "app": [app_name] }
            ]
        })
        .to_string()
    }

    #[cfg(target_os = "macos")]
    fn ghostty_style_cask_json(mock_server_uri: &str, version: &str, sha256: &str) -> String {
        serde_json::json!({
            "token": "ghostty",
            "version": version,
            "url": format!("{mock_server_uri}/ghostty-{version}.dmg"),
            "sha256": sha256,
            "artifacts": [
                { "app": ["Ghostty.app"] },
                { "manpage": ["$APPDIR/Ghostty.app/Contents/Resources/man/man1/ghostty.1"] },
                { "manpage": ["$APPDIR/Ghostty.app/Contents/Resources/man/man5/ghostty.5"] },
                { "bash_completion": ["$APPDIR/Ghostty.app/Contents/Resources/bash-completion/completions/ghostty.bash"] },
                { "fish_completion": ["$APPDIR/Ghostty.app/Contents/Resources/fish/vendor_completions.d/ghostty.fish"] },
                { "zsh_completion": ["$APPDIR/Ghostty.app/Contents/Resources/zsh/site-functions/_ghostty"] },
                { "zap": [{ "trash": ["~/.config/ghostty"], "rmdir": ["~/Library/Caches/Ghostty"] }] }
            ]
        })
        .to_string()
    }

    #[cfg(target_os = "macos")]
    fn make_installer(root: &Path, prefix: &Path, cask_base_url: String) -> Installer {
        fs::create_dir_all(root.join("db")).unwrap();
        let api_client = ApiClient::with_base_url(format!("{}/formula", cask_base_url))
            .unwrap()
            .with_cask_base_url(cask_base_url);
        let blob_cache = BlobCache::new(&root.join("cache")).unwrap();
        let store = Store::new(root).unwrap();
        let cellar = Cellar::new(root).unwrap();
        let linker = Linker::new(prefix).unwrap();
        let db = Database::open(&root.join("db/zb.sqlite3")).unwrap();

        Installer::new(
            api_client,
            blob_cache,
            store,
            cellar,
            linker,
            db,
            prefix.to_path_buf(),
            root.join("locks"),
        )
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn install_cask_dmg_places_app_and_uninstalls_cleanly() {
        let _home_lock = HOME_ENV_LOCK.lock().unwrap();
        let mock_server = MockServer::start().await;
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let _home_override = HomeOverride::set(&home);

        let dmg = create_cask_dmg("Zed.app", "Contents/MacOS/cli", "#!/bin/sh\necho zed");
        let dmg_sha = sha256_hex(&dmg);

        Mock::given(method("GET"))
            .and(path("/zed.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(cask_json(
                &mock_server.uri(),
                "1.0.0",
                &dmg_sha,
            )))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/downloads/zed-1.0.0.dmg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(dmg))
            .mount(&mock_server)
            .await;

        let root = tmp.path().join("zerobrew");
        let prefix = tmp.path().join("prefix");
        let mut installer = make_installer(&root, &prefix, mock_server.uri());

        installer
            .install(&["cask:zed".to_string()], true)
            .await
            .unwrap();

        let app_binary = home.join("Applications/Zed.app/Contents/MacOS/cli");
        assert!(app_binary.exists());
        assert!(prefix.join("bin/zed").exists());
        assert_eq!(
            fs::canonicalize(prefix.join("bin/zed")).unwrap(),
            fs::canonicalize(&app_binary).unwrap()
        );
        assert!(
            installer
                .db
                .list_keg_files_for_name("cask:zed")
                .unwrap()
                .iter()
                .any(|record| record.linked_path
                    == home.join("Applications/Zed.app").display().to_string())
        );

        installer.uninstall("cask:zed").unwrap();
        assert!(!home.join("Applications/Zed.app").exists());
        assert!(!prefix.join("bin/zed").exists());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn install_app_only_cask_places_app_without_binary_links() {
        let _home_lock = HOME_ENV_LOCK.lock().unwrap();
        let mock_server = MockServer::start().await;
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let _home_override = HomeOverride::set(&home);

        let dmg = create_cask_dmg(
            "Brave Browser.app",
            "Contents/MacOS/Brave Browser",
            "#!/bin/sh\necho brave",
        );
        let dmg_sha = sha256_hex(&dmg);

        Mock::given(method("GET"))
            .and(path("/brave-browser.json"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(app_only_cask_json(
                    &mock_server.uri(),
                    "brave-browser",
                    "Brave Browser.app",
                    "1.0.0",
                    &dmg_sha,
                )),
            )
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/brave-browser-1.0.0.dmg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(dmg))
            .mount(&mock_server)
            .await;

        let root = tmp.path().join("zerobrew");
        let prefix = tmp.path().join("prefix");
        let mut installer = make_installer(&root, &prefix, mock_server.uri());

        installer
            .install(&["cask:brave-browser".to_string()], true)
            .await
            .unwrap();

        assert!(home.join("Applications/Brave Browser.app").exists());
        assert!(!prefix.join("bin/brave-browser").exists());

        installer.uninstall("cask:brave-browser").unwrap();
        assert!(!home.join("Applications/Brave Browser.app").exists());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn install_ghostty_style_cask_ignores_extra_artifacts_and_uninstalls_with_zap() {
        let _home_lock = HOME_ENV_LOCK.lock().unwrap();
        let mock_server = MockServer::start().await;
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let _home_override = HomeOverride::set(&home);

        let dmg = create_cask_dmg(
            "Ghostty.app",
            "Contents/MacOS/ghostty",
            "#!/bin/sh\necho ghostty",
        );
        let dmg_sha = sha256_hex(&dmg);

        Mock::given(method("GET"))
            .and(path("/ghostty.json"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(ghostty_style_cask_json(
                    &mock_server.uri(),
                    "1.0.0",
                    &dmg_sha,
                )),
            )
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/ghostty-1.0.0.dmg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(dmg))
            .mount(&mock_server)
            .await;

        let root = tmp.path().join("zerobrew");
        let prefix = tmp.path().join("prefix");
        let mut installer = make_installer(&root, &prefix, mock_server.uri());

        installer
            .install(&["cask:ghostty".to_string()], true)
            .await
            .unwrap();

        let zap_file = home.join(".config/ghostty");
        let zap_dir = home.join("Library/Caches/Ghostty");
        fs::create_dir_all(zap_file.parent().unwrap()).unwrap();
        fs::create_dir_all(zap_dir.join("nested")).unwrap();
        fs::write(&zap_file, "ghostty config").unwrap();

        installer.uninstall("cask:ghostty").unwrap();

        assert!(!home.join("Applications/Ghostty.app").exists());
        assert!(!zap_file.exists());
        assert!(!zap_dir.exists());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn install_cask_dmg_honors_no_link() {
        let _home_lock = HOME_ENV_LOCK.lock().unwrap();
        let mock_server = MockServer::start().await;
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let _home_override = HomeOverride::set(&home);

        let dmg = create_cask_dmg("Zed.app", "Contents/MacOS/cli", "#!/bin/sh\necho zed");
        let dmg_sha = sha256_hex(&dmg);

        Mock::given(method("GET"))
            .and(path("/zed.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(cask_json(
                &mock_server.uri(),
                "1.0.0",
                &dmg_sha,
            )))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/downloads/zed-1.0.0.dmg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(dmg))
            .mount(&mock_server)
            .await;

        let root = tmp.path().join("zerobrew");
        let prefix = tmp.path().join("prefix");
        let mut installer = make_installer(&root, &prefix, mock_server.uri());

        installer
            .install(&["cask:zed".to_string()], false)
            .await
            .unwrap();

        assert!(
            home.join("Applications/Zed.app/Contents/MacOS/cli")
                .exists()
        );
        assert!(!prefix.join("bin/zed").exists());
        assert!(root.join("cellar/cask:zed/1.0.0/bin/zed").exists());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn reinstall_cask_replaces_owned_app_bundle() {
        let _home_lock = HOME_ENV_LOCK.lock().unwrap();
        let first_server = MockServer::start().await;
        let second_server = MockServer::start().await;
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let _home_override = HomeOverride::set(&home);

        let first_dmg = create_cask_dmg("Zed.app", "Contents/MacOS/cli", "#!/bin/sh\necho first");
        let first_sha = sha256_hex(&first_dmg);
        Mock::given(method("GET"))
            .and(path("/zed.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(cask_json(
                &first_server.uri(),
                "1.0.0",
                &first_sha,
            )))
            .mount(&first_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/downloads/zed-1.0.0.dmg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(first_dmg))
            .mount(&first_server)
            .await;

        let second_dmg = create_cask_dmg("Zed.app", "Contents/MacOS/cli", "#!/bin/sh\necho second");
        let second_sha = sha256_hex(&second_dmg);
        Mock::given(method("GET"))
            .and(path("/zed.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(cask_json(
                &second_server.uri(),
                "1.0.0",
                &second_sha,
            )))
            .mount(&second_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/downloads/zed-1.0.0.dmg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(second_dmg))
            .mount(&second_server)
            .await;

        let root = tmp.path().join("zerobrew");
        let prefix = tmp.path().join("prefix");
        let mut installer = make_installer(&root, &prefix, first_server.uri());
        installer
            .install(&["cask:zed".to_string()], true)
            .await
            .unwrap();

        let mut installer = make_installer(&root, &prefix, second_server.uri());
        installer
            .install(&["cask:zed".to_string()], true)
            .await
            .unwrap();

        let contents =
            fs::read_to_string(home.join("Applications/Zed.app/Contents/MacOS/cli")).unwrap();
        assert!(contents.contains("second"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn install_cask_reports_conflict_for_unowned_app_bundle() {
        let _home_lock = HOME_ENV_LOCK.lock().unwrap();
        let mock_server = MockServer::start().await;
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(home.join("Applications/Zed.app")).unwrap();
        let _home_override = HomeOverride::set(&home);

        let dmg = create_cask_dmg("Zed.app", "Contents/MacOS/cli", "#!/bin/sh\necho zed");
        let dmg_sha = sha256_hex(&dmg);

        Mock::given(method("GET"))
            .and(path("/zed.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(cask_json(
                &mock_server.uri(),
                "1.0.0",
                &dmg_sha,
            )))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/downloads/zed-1.0.0.dmg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(dmg))
            .mount(&mock_server)
            .await;

        let root = tmp.path().join("zerobrew");
        let prefix = tmp.path().join("prefix");
        let mut installer = make_installer(&root, &prefix, mock_server.uri());

        let err = match installer.install(&["cask:zed".to_string()], true).await {
            Ok(_) => panic!("expected install to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::LinkConflict { .. }));
    }
}
