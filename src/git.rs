use git2::DiffOptions;
use git2::Repository;

fn main() -> Result<(), git2::Error> {
    // 1. Open the repository
    let repo = Repository::open(".")?;

    // 2. Specify the branch name you want to compare against
    let branch_name = "main";

    // 3. Resolve the branch name to its latest commit tree object
    let obj = repo.revparse_single(branch_name)?;
    let commit = obj.as_commit().ok_or_else(|| {
        git2::Error::from_str("The specified branch target is not a valid commit")
    })?;
    let tree = commit.tree()?;

    // 4. Configure your diff options (e.g., to include untracked files)
    let mut opts = DiffOptions::new();
    opts.include_untracked(true); // Optional: mimics `git diff HEAD` behavior

    // 5. Generate the diff between the branch tree and the working tree
    let diff = repo.diff_tree_to_workdir_with_index(Some(&tree), Some(&mut opts))?;

    // 6. Print the output in standard patch format
    diff.print(git2::DiffFormat::Patch, |_delta, _hunk, line| {
        if let Ok(content) = std::str::from_utf8(line.content()) {
            print!("{}{}", line.origin(), content);
        }
        true
    })?;

    Ok(())
}
