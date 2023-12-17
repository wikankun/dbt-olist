# DBT Learning Repo

## How to Run

Create and activate virtual environment
```
virtualenv env
. env/bin/activate
```

Install DBT
```
pip install dbt-postgres
```

Config your profiles.yml [Reference](https://docs.getdbt.com/dbt-cli/configure-your-profile)
```
nano /home/{your_username}/.dbt/profiles.yml
```

Enter olist directory
```
cd olist
```

Run

```
dbt build --select history wh sources
```

```
dbt run --select history wh
```

```
dbt test
```

```
dbt docs generate
dbt docs serve
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
