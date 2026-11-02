# Apache Spark lab with Docker Compose: learn DataFrames and Spark SQL on a CSV file

[English](README.md) | [Français](README.fr.md) | [日本語](README.ja.md) | [Español](README.es.md)

[![check](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml/badge.svg)](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml)

A ready-made **Apache Spark lab** for classrooms, from the [MitsuruTech Learning Platform](https://mitsurutech.co.uk/learning), where schools and colleges give every student their own databases, containers and terminal in the browser. Each student gets their own small Spark with a CSV file already loaded as a **DataFrame**: they filter it, group and count it, answer a question in **Spark SQL**, and then open the **Spark UI** to see the jobs their queries made. Nothing to install, and no cluster to book.

It is one `docker-compose` file, `compose.yaml`, and it runs the same on your own machine as it does on the platform, where every student gets a private copy with no way out to the internet.

## What is in it

| Service | Image | Memory | What it is |
| --- | --- | --- | --- |
| `spark` | [`spark:4.0.1-scala2.13-java21-python3-ubuntu`](https://hub.docker.com/_/spark) | 512 MB | Apache Spark 4.0.1 in local mode, with a small web service in front of it |

The one service starts Spark (the driver and two executor threads in a single process, which is what fits a small copy), writes `orders.csv` (24 orders with six columns: `order_id`, `customer`, `country`, `product`, `quantity`, `unit_price`), loads it into a DataFrame called `orders` and a SQL table of the same name, and waits for you. It is healthy in about six seconds and uses about 380 MB when idle, which is most of its 512 MB: the data is tiny, and this is a lab for learning the API, not for big data.

It answers on two ports, both inside the copy:

- **4040, the Spark UI.** The platform's lab page opens it for you. Its Jobs, Stages and SQL tabs show every query as it runs.
- **8000, a small query service.** Post a Spark SQL query to `/sql`, one DataFrame expression to `/df`, or a few lines of PySpark to `/py`, and the result comes back as a table. `/py` runs the script with `orders`, `F` and `spark` in scope and answers what it printed, then the value of its last line. It is there because the terminal that sits beside a lab has `curl` and no Spark of its own.

The image is Apache Spark's own, run unmodified and pinned to a version (never `latest`), so the lab does not change under a class between one lesson and the next. Nothing is built here and nothing from this repository is distributed in it. One setting is worth knowing about: Spark refuses to start in a heap this small unless told otherwise, so the file sets `spark.testing.reservedMemory`, a setting meant for Spark's own tests, to fit the 512 MB limit. Do not copy it to a real cluster.

## Using it on the platform

The [MitsuruTech Learning Platform](https://mitsurutech.co.uk/learning) gives every student their own copy of a lab, in the browser, with a terminal beside it.

**To try it:** open the [container page](https://mitsurutech.co.uk/container), find this lab in **Sample labs** and press Start. No account is needed to try one.

**To set it as work for a class:**

1. In your module, make a task of type **Lab** and choose **In a GitHub repository**.
2. Give `https://github.com/mitsurutech/lab-spark` with the branch `main`. The compose file is the default, `compose.yaml`. Fork the repository first if you want to change the file or the data.
3. Publish. Every student's copy starts from the commit you published, so a later change here does not alter a lab already set.
4. Say in the task what to hand in (see [For teachers](#for-teachers)), and assign it like any other task.

The help manual's [page for this lab](https://mitsurutech.co.uk/help/lab-spark) and its [guide to container labs](https://mitsurutech.co.uk/help/labs) cover the rest: what a compose file may say, how much memory a copy gets, and how a teacher opens, watches and marks a student's copy.

No college account yet? [Your first year is free](https://mitsurutech.co.uk/pricing) for up to 50 students, and [signing up](https://mitsurutech.co.uk/signup) takes an email address.

## Running it on your own machine

You need Docker with the Compose plugin. The first start downloads the image, which is about 800 MB.

```bash
docker compose -f compose.yaml -f local.yaml up
```

Open <http://localhost:4040> for the Spark UI, and in another terminal ask it something:

```bash
curl -s -d "SELECT country, count(*) AS orders FROM orders GROUP BY country ORDER BY orders DESC" http://localhost:8000/sql
```

`local.yaml` publishes both ports on `127.0.0.1` only, so the lab is reachable from your own computer and from nowhere else. Stop it with `Ctrl-C`, and remove it with `docker compose -f compose.yaml -f local.yaml down -v`.

## Working through it

1. **Look at the data.** `SELECT * FROM orders LIMIT 5` to `/sql` shows the first rows, and `orders.dtypes` to `/df` shows the types Spark worked out from the file.
2. **Filter.** `orders.filter(orders.country == "UK")` to `/df` keeps the UK orders: seven of the 24.
3. **Group and count.** `orders.groupBy("country").count()` to `/df`: UK has seven orders, Japan and France six, Spain five.
4. **Ask it in SQL.** `SELECT country, round(sum(quantity * unit_price), 2) AS revenue FROM orders GROUP BY country ORDER BY revenue DESC` to `/sql` gives Japan 717.5, UK 670.0, France 495.0 and Spain 490.0.
5. **See the jobs.** Open the Spark UI and look at the SQL / DataFrame tab: every query you ran is there, with its plan, and the Jobs tab has the work it was split into.
6. **Run a script.** Three lines to `/py` (`by = orders.groupBy("country").agg(F.sum("quantity").alias("units"))`, `print("countries:", by.count())` and `by.orderBy(F.desc("units"))`, each on its own line) answer `countries: 4` and then a table with Japan on top at 17 units, then France 15, Spain 12 and UK 11.

## For teachers

Nothing in this lab is marked automatically. The student hands in what they found and you mark it, the way you mark any other lab.

**Ask for a result and a sentence.** A query the student wrote themselves over `orders.csv` (the revenue per country, the most ordered product, the biggest order), the table it printed, and a screenshot of that query in the Spark UI with the lab's own address visible so it is clearly their copy. Then a sentence or two in their own words on what Spark did: it read the file, built a plan, and ran it as a job. The wording tells you whether it was understood.

**Where it sits in a course.** The lab fits data engineering and big data units: DataFrames against SQL as two ways of asking one question, lazy evaluation and plans, and what a job is. Check your own awarding body's unit for the exact wording; none is quoted here.

**Who sees what.** A copy is private to the student who started it. A member of staff who teaches the module can open it to see what the student has done, and opening a student's work is written to the access log, which the student can read.

## What does not work in a lab

This is one Spark in local mode, not a cluster: there is no master and worker to start, stop or break, and no data spread over machines. The data is a 24-row file, so timings say nothing about real workloads, and a query that needs far more memory than the copy has will fail. The `/df` endpoint evaluates one Python expression and the `/py` endpoint runs a short Python script, both with Spark's functions, which is for learning the DataFrame API in your own private copy with no way out, and neither is a service to expose. The lab has no way out to the internet at all, so nothing can be downloaded into it, and it forgets every query when it stops.

## Troubleshooting

- **The Spark UI does not appear straight after Start.** Wait a few seconds and press Open again: the lab reports itself ready only once Spark has loaded the file.
- **A query answers with an error.** The first line says what Spark did not understand, for example a column that does not exist; the columns are listed in the message.
- **On your own machine, port 4040 or 8000 is already in use.** Change the left-hand number in `local.yaml` and open that port instead.

## Checking the lab

`test/check.sh` starts the lab the way the platform's runner starts every service (all capabilities dropped, `no-new-privileges`, a process limit, memory limits, and no way out to the internet) and then checks from inside its network what this page promises: the 24 rows, the filter, the group and count, the SQL answer, the Spark UI listing the jobs. It needs Docker and runs on every push, which is the badge at the top.

```bash
test/check.sh
```

## Versions and licences

[Apache Spark](https://spark.apache.org) is Apache-2.0 licensed. The image is `spark:4.0.1-scala2.13-java21-python3-ubuntu`, the Apache Spark project's own Docker Official Image.

The application inside is the licence of its own project, linked above.

## Licence

This work is licensed under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). You may share it and adapt it for learning and teaching, if you credit Mitsuru Technologies Ltd, do not use it commercially, and share anything you make from it under the same licence. The full text is in the `LICENSE` file.

## About

Made by [Mitsuru Technologies](https://mitsurutech.co.uk) for the [MitsuruTech Learning Platform](https://mitsurutech.co.uk/learning): Python, Java, Go, Node, SQL and Linux in the browser for schools and colleges, with container labs, web apps deployed from GitHub, and quizzes that mark themselves. Try code with no account in the [playground](https://mitsurutech.co.uk/playground).

More labs from the same place: [lab-juice-shop](https://github.com/mitsurutech/lab-juice-shop), [lab-dvwa](https://github.com/mitsurutech/lab-dvwa), [lab-webgoat](https://github.com/mitsurutech/lab-webgoat), [lab-mysql-phpmyadmin](https://github.com/mitsurutech/lab-mysql-phpmyadmin), [lab-git-gitea](https://github.com/mitsurutech/lab-git-gitea) and [lab-sample](https://github.com/mitsurutech/lab-sample).
