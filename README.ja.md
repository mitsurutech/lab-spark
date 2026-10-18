# Docker Compose で動かす Apache Spark ラボ：CSV ファイルで DataFrame と Spark SQL を学ぶ

[English](README.md) | [Français](README.fr.md) | [日本語](README.ja.md) | [Español](README.es.md)

[![check](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml/badge.svg)](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml)

授業でそのまま使える **Apache Spark ラボ**です。学校や学校法人が、生徒一人ひとりにデータベース、コンテナー、ブラウザー上のターミナルを用意できる [MitsuruTech ラーニングプラットフォーム](https://mitsurutech.co.uk/ja/learning)から提供しています。生徒はそれぞれ、CSV ファイルが **DataFrame** として読み込み済みの小さな専用の Spark を使います。絞り込み、グループ化して数え、**Spark SQL** で問いに答え、そのあと **Spark UI** を開いて、自分のクエリが作ったジョブを確認します。インストールは不要で、クラスターを予約する必要もありません。

ファイルは `docker-compose` ファイル 1 つ、`compose.yaml` だけです。自分のマシンでもプラットフォームでも同じように動きます。プラットフォームでは、生徒ごとに専用のコピーが用意され、インターネットへの出口はありません。

## 中身

| サービス | イメージ | メモリ | 役割 |
| --- | --- | --- | --- |
| `spark` | [`spark:4.0.1-scala2.13-java21-python3-ubuntu`](https://hub.docker.com/_/spark) | 512 MB | ローカルモードの Apache Spark 4.0.1 と、その前に置いた小さな Web サービスです |

この 1 つのサービスが、Spark（ドライバーと 2 つのエグゼキュータースレッドを 1 つのプロセスで動かします。小さなコピーに収まる構成です）を起動し、`orders.csv`（6 列、24 件の注文：`order_id`、`customer`、`country`、`product`、`quantity`、`unit_price`）を書き出し、`orders` という名前の DataFrame と同名の SQL テーブルに読み込んで、待機します。約 6 秒で正常に動き始め、待機中の使用量は約 380 MB で、512 MB の大半を占めます。データはごく小さく、これはビッグデータのためではなく、API を学ぶためのラボです。

コピーの内側で、2 つのポートが応答します。

- **4040、Spark UI。**プラットフォームのラボのページが開いてくれます。Jobs、Stages、SQL の各タブに、実行中のすべてのクエリが表示されます。
- **8000、小さなクエリサービス。**Spark SQL のクエリを `/sql` に、DataFrame の式を `/df` に POST すると、結果が表になって返ります。ラボの横にあるターミナルには `curl` はあっても Spark 自体がないため、このサービスがあります。

イメージは Apache Spark 自身のものを、変更せずにバージョンを固定して（`latest` は使いません）実行します。そのため、レッスンとレッスンの間でラボが変わることはありません。ここでは何もビルドせず、このリポジトリのものがイメージの中に配布されることもありません。知っておくべき設定が 1 つあります。Spark は、指定しないとこれほど小さなヒープでは起動を拒否するため、ファイルでは 512 MB の制限に収まるように、Spark 自身のテスト向けの設定である `spark.testing.reservedMemory` を設定しています。実際のクラスターにはコピーしないでください。

## プラットフォームで使う

[MitsuruTech ラーニングプラットフォーム](https://mitsurutech.co.uk/ja/learning)では、生徒一人ひとりがブラウザー上で、ターミナルを横に置いたラボの専用コピーを使えます。

**試すには：**[コンテナーのページ](https://mitsurutech.co.uk/ja/container)を開き、**サンプルラボ**からこのラボを選んで「起動」を押します。試すだけならアカウントは不要です。

**クラスの課題として出すには：**

1. モジュールで、種類が**ラボ**の課題を作り、**GitHub リポジトリ**を選びます。
2. `https://github.com/mitsurutech/lab-spark` とブランチ `main` を指定します。compose ファイルは既定の `compose.yaml` です。ファイルやデータを変更したい場合は、先にリポジトリをフォークしてください。
3. 公開します。生徒のコピーはどれも、公開した時点のコミットから始まるので、あとでここを変更しても、すでに出したラボは変わりません。
4. 何を提出するかを課題に書き（[教員の方へ](#教員の方へ)を参照）、ほかの課題と同じように配布します。

マニュアルの[このラボのページ](https://mitsurutech.co.uk/ja/help/lab-spark)と[コンテナーラボのガイド](https://mitsurutech.co.uk/ja/help/labs)に、そのほかのことが載っています。compose ファイルに書けること、コピーが使えるメモリの量、教員が生徒のコピーを開いて見て採点する方法などです。

学校のアカウントはまだありませんか？[最初の 1 年は無料](https://mitsurutech.co.uk/ja/pricing)で、生徒 50 人まで使えます。[登録](https://mitsurutech.co.uk/ja/signup)にはメールアドレスだけが必要です。

## 自分のマシンで動かす

Compose プラグイン付きの Docker が必要です。初回の起動では、約 800 MB のイメージをダウンロードします。

```bash
docker compose -f compose.yaml -f local.yaml up
```

Spark UI は <http://localhost:4040> で開きます。別のターミナルから、次のように問い合わせます。

```bash
curl -s -d "SELECT country, count(*) AS orders FROM orders GROUP BY country ORDER BY orders DESC" http://localhost:8000/sql
```

`local.yaml` は両方のポートを `127.0.0.1` に限って公開するので、ラボには自分のコンピューターからしかアクセスできません。`Ctrl-C` で止め、`docker compose -f compose.yaml -f local.yaml down -v` で削除します。

## 進め方

1. **データを見ます。**`/sql` に `SELECT * FROM orders LIMIT 5` を送ると最初の数行が表示され、`/df` に `orders.dtypes` を送ると、Spark がファイルから判断した型が表示されます。
2. **絞り込みます。**`/df` に `orders.filter(orders.country == "UK")` を送ると、UK の注文だけが残ります。24 件のうち 7 件です。
3. **グループ化して数えます。**`/df` に `orders.groupBy("country").count()` を送ります。UK は 7 件、日本とフランスは 6 件、スペインは 5 件です。
4. **SQL で問い合わせます。**`/sql` に `SELECT country, round(sum(quantity * unit_price), 2) AS revenue FROM orders GROUP BY country ORDER BY revenue DESC` を送ると、日本 717.5、UK 670.0、フランス 495.0、スペイン 490.0 が返ります。
5. **ジョブを見ます。**Spark UI を開き、SQL / DataFrame タブを見ます。実行したクエリがすべて、その実行計画とともに表示されています。Jobs タブには、それが分割された作業があります。

## 教員の方へ

このラボには自動採点がありません。生徒が見つけたことを提出し、教員が、ほかのラボと同じように採点します。

**結果と 1 文を求めてください。**生徒が `orders.csv` に対して自分で書いたクエリ（国ごとの売上、最も多く注文された商品、最大の注文など）、それが表示した表、そしてそのクエリを Spark UI で表示したスクリーンショットです。スクリーンショットには、そのラボ自身のアドレスが見えるようにして、生徒自身のコピーであることを明らかにします。続けて、Spark が何をしたかを、自分の言葉で 1、2 文で書いてもらいます。ファイルを読み、実行計画を作り、ジョブとして実行した、という内容です。その表現から、理解できたかどうかが分かります。

**コースでの位置づけ。**データエンジニアリングとビッグデータの単元に向いています。同じ問いに対する 2 つの尋ね方としての DataFrame と SQL、遅延評価と実行計画、ジョブとは何か、を学べます。正確な表現は、ご自身の認定団体の単元でご確認ください。ここでは引用していません。

**誰が何を見られるか。**コピーは、起動した生徒だけのものです。そのモジュールを教えている教職員は、生徒の作業内容を確認するためにコピーを開けます。生徒の作業を開いたことはアクセスログに記録され、生徒はそれを読めます。

## ラボでできないこと

これはクラスターではなく、ローカルモードの Spark 1 つです。起動、停止、故障させるマスターやワーカーはなく、複数のマシンにデータが分散することもありません。データは 24 行のファイルなので、処理時間は実際の負荷について何も語りません。コピーが持つよりはるかに多くのメモリを必要とするクエリは失敗します。`/df` エンドポイントは Spark の関数を使った Python の式を 1 つ評価します。これは自分専用のコピーで DataFrame API を学ぶためのもので、公開するサービスではありません。ラボにはインターネットへの出口がまったくないので、何もダウンロードできず、止めるとすべてのクエリが忘れられます。

## トラブルシューティング

- **「起動」の直後に Spark UI が表示されません。**数秒待ってから、もう一度「開く」を押してください。ラボは、Spark がファイルを読み込んでから、初めて準備完了と表示します。
- **クエリがエラーを返します。**最初の行に、Spark が理解できなかった点（たとえば存在しない列）が書かれています。列の一覧もメッセージに含まれています。
- **自分のマシンで、ポート 4040 または 8000 がすでに使われています。**`local.yaml` の左側の数字を変え、代わりにそのポートを開いてください。

## ラボの検証

`test/check.sh` は、プラットフォームのランナーがすべてのサービスを起動するのと同じ条件（すべてのケーパビリティを削除、`no-new-privileges`、プロセス数の制限、メモリの制限、インターネットへの出口なし）でラボを起動し、そのネットワークの内側から、このページの内容を確認します。24 行あること、絞り込み、グループ化と集計、SQL の答え、Spark UI がジョブを一覧することです。Docker が必要で、push のたびに実行されます。ページ上部のバッジがそれです。

```bash
test/check.sh
```

## バージョンとライセンス

[Apache Spark](https://spark.apache.org) は Apache-2.0 でライセンスされています。イメージは `spark:4.0.1-scala2.13-java21-python3-ubuntu` で、Apache Spark プロジェクト自身の Docker Official Image です。

中のアプリケーションは、上にリンクした、そのプロジェクトのライセンスに従います。

## ライセンス

この作品は [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) でライセンスされています。Mitsuru Technologies Ltd のクレジットを表示し、商用に使わず、これをもとに作ったものを同じライセンスで共有するなら、学習と授業のために共有・改変できます。全文は `LICENSE` ファイルにあります。

## この演習について

[Mitsuru Technologies](https://mitsurutech.co.uk) が、[MitsuruTech ラーニングプラットフォーム](https://mitsurutech.co.uk/ja/learning)のために作りました。学校向けに、Python、Java、Go、Node、SQL、Linux をブラウザーで使えるようにし、コンテナーラボ、GitHub から公開する Web アプリ、自動採点されるクイズも備えています。アカウントなしでコードを試せる[プレイグラウンド](https://mitsurutech.co.uk/ja/playground)もあります。

同じ提供元のほかのラボ：[lab-juice-shop](https://github.com/mitsurutech/lab-juice-shop)、[lab-dvwa](https://github.com/mitsurutech/lab-dvwa)、[lab-webgoat](https://github.com/mitsurutech/lab-webgoat)、[lab-mysql-phpmyadmin](https://github.com/mitsurutech/lab-mysql-phpmyadmin)、[lab-git-gitea](https://github.com/mitsurutech/lab-git-gitea)、[lab-sample](https://github.com/mitsurutech/lab-sample)。
