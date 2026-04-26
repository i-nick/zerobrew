use console::style;

pub fn execute(installer: &mut zb_io::Installer, all: bool) -> Result<(), zb_core::Error> {
    let installed = if all {
        installer.list_installed()?
    } else {
        installer.list_requested_installed()?
    };

    if installed.is_empty() {
        println!("No formulas installed.");
    } else {
        for keg in installed {
            println!("{} {}", style(&keg.name).bold(), style(&keg.version).dim());
        }
    }

    Ok(())
}
