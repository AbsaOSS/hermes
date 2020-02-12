# Github Pages Starter

This repositories purpose is to serve as a starter of github pages for any project of AbsaOSS

### Dependencies:
- ruby >= 2.3.0

### To build locally:
```bash
# In the root of the project
$> git checkout gh-pages
$> bundle install
$> bundle exec jekyll serve
# => Now browse to http://localhost:4000
```

### Run convinience scripts
#### Init new docs
- Name - Name of the project
- Description - Description of the project
- Version - on which version should docs start

```ruby
ruby utils/init_docs.rb <name> <description> <optional_version>
```

#### Add/Remove topic
Automatically add or remove topic with adding it to menu as well

- Action - add or remove
- Topic Name - topic name in the "file form" so Build Process would be build-process

```ruby
ruby utils/manage_topics.rb <action> <topic_name>
```

#### Generate new docs versiom
Generates new docs version folder with the specified version

```ruby
ruby utils/create_docs.rb <version>
```

#### Generate release notes
A Ruby script to generate release notes.

```bash
Usage: ruby utils/get_release_notes.rb VERSION [options]

Specific options:
        --github-token TOKEN         Github token. Can be specified using environment variable GITHUB_TOKEN or in get_release_notes.json in resources using github_token key
        --zenhub-token TOKEN         Zenhub token. This means we will use Release object for release notes. You don't have to use --use-zenhub in case you do this. Can be specified using environment variable ZENHUB_TOKEN or in get_release_notes.json in resources using zenhub_token key
    -z, --use-zenhub                 Run using zenhub. It needs zenhub token set. If you use --zenhub-token option, you don't need to use this. This means we will use Release object for release notes.
        --organization ORGANIZATION  Github Organization
        --repository REPOSITORY      Github Repository name
        --repository-id REPOSITORYID Zenhub Repository ID
        --zenhub-url ZENURL          Zenhub API URL
        --github-url GITURL          Github API URL
    -p, --[no-]print-empty           Should Issue with no release notes comment be included in the output file
        --[no-]print-only-title      Should Issue with no release notes comment be preceeded with 'Couldn't find comment'
    -s, --[no-]strict                Treats warnings as errors
    -h, --help                       Show this message
```
