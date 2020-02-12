using Documenter, Format2DB

makedocs(
    modules = [Format2DB],
    format = Documenter.HTML(; prettyurls = get(ENV, "CI", nothing) == "true"),
    authors = "yakir12",
    sitename = "Format2DB.jl",
    pages = Any["index.md"]
    # strict = true,
    # clean = true,
    # checkdocs = :exports,
)

deploydocs(
    repo = "github.com/yakir12/Format2DB.jl.git",
    push_preview = true
)
