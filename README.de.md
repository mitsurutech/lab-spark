# Apache-Spark-Lab mit Docker Compose: DataFrames und Spark SQL an einer CSV-Datei lernen

[English](README.md) | [Français](README.fr.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Deutsch](README.de.md)

[![check](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml/badge.svg)](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml)

Ein fertiges **Apache-Spark-Lab** für den Unterricht, von der [MitsuruTech Learning Platform](https://mitsurutech.co.uk/learning), auf der Schulen und Hochschulen jeder lernenden Person eigene Datenbanken, Container und ein Terminal im Browser geben. Jede Person bekommt ihr eigenes kleines Spark mit einer CSV-Datei, die schon als **DataFrame** geladen ist: Sie filtert sie, gruppiert und zählt sie, beantwortet eine Frage in **Spark SQL** und öffnet dann die **Spark UI**, um die Jobs zu sehen, die ihre Abfragen erzeugt haben. Nichts zu installieren, und kein Cluster zu buchen.

Es ist eine einzige `docker-compose`-Datei, `compose.yaml`, und sie läuft auf deinem Rechner genauso wie auf der Plattform, wo jede Person eine private Kopie ohne Weg ins Internet bekommt.

## Was drin ist

| Dienst | Image | Speicher | Was es ist |
| --- | --- | --- | --- |
| `spark` | [`spark:4.0.1-scala2.13-java21-python3-ubuntu`](https://hub.docker.com/_/spark) | 512 MB | Apache Spark 4.0.1 im lokalen Modus, mit einem kleinen Webdienst davor |

Der eine Dienst startet Spark (den Driver und zwei Executor-Threads in einem einzigen Prozess, was zu einer kleinen Kopie passt), schreibt `orders.csv` (24 Bestellungen mit sechs Spalten: `order_id`, `customer`, `country`, `product`, `quantity`, `unit_price`), lädt sie in einen DataFrame namens `orders` und in eine SQL-Tabelle desselben Namens und wartet auf dich. Er ist nach etwa sechs Sekunden bereit und braucht im Leerlauf rund 380 MB, also den größten Teil seiner 512 MB: Die Daten sind winzig, und dies ist ein Lab zum Lernen der API, nicht für Big Data.

Er antwortet auf zwei Ports, beide innerhalb der Kopie:

- **4040, die Spark UI.** Die Lab-Seite der Plattform öffnet sie für dich. Ihre Reiter Jobs, Stages und SQL zeigen jede Abfrage, während sie läuft.
- **8000, ein kleiner Abfragedienst.** Schicke eine Spark-SQL-Abfrage an `/sql`, einen DataFrame-Ausdruck an `/df` oder ein paar Zeilen PySpark an `/py`, und das Ergebnis kommt als Tabelle zurück. `/py` führt das Skript mit `orders`, `F` und `spark` im Geltungsbereich aus und antwortet mit dem, was es ausgegeben hat, und danach mit dem Wert der letzten Zeile. Es gibt ihn, weil das Terminal neben einem Lab `curl` hat und kein eigenes Spark.

Das Image ist das von Apache Spark selbst, unverändert und auf eine Version festgelegt (nie `latest`), damit sich das Lab zwischen zwei Stunden nicht unter einer Klasse ändert. Hier wird nichts gebaut, und nichts aus diesem Repository wird darin verteilt. Eine Einstellung sollte man kennen: Spark weigert sich, in einem so kleinen Heap zu starten, wenn man es nicht anders einstellt. Darum setzt die Datei `spark.testing.reservedMemory`, eine Einstellung für Sparks eigene Tests, damit es in die 512 MB passt. Übernimm sie nicht für einen echten Cluster.

## Auf der Plattform benutzen

Die [MitsuruTech Learning Platform](https://mitsurutech.co.uk/learning) gibt jeder lernenden Person ihre eigene Kopie eines Labs, im Browser, mit einem Terminal daneben.

**Zum Ausprobieren:** Öffne die [Container-Seite](https://mitsurutech.co.uk/container), suche dieses Lab unter **Sample labs** und drücke Start. Zum Ausprobieren ist kein Konto nötig.

**Um es einer Klasse als Aufgabe zu geben:**

1. Lege in deinem Modul eine Aufgabe vom Typ **Lab** an und wähle **In einem GitHub-Repository**.
2. Gib `https://github.com/mitsurutech/lab-spark` mit dem Branch `main` an. Die Compose-Datei ist standardmäßig `compose.yaml`. Forke das Repository zuerst, wenn du die Datei oder die Daten ändern willst.
3. Veröffentliche. Die Kopie jeder Person startet vom veröffentlichten Commit, eine spätere Änderung hier ändert also kein Lab, das schon gestellt wurde.
4. Schreibe in die Aufgabe, was abzugeben ist (siehe [Für Lehrkräfte](#für-lehrkräfte)), und weise sie wie jede andere Aufgabe zu.

Die [Seite zu diesem Lab](https://mitsurutech.co.uk/help/lab-spark) im Hilfehandbuch und die [Anleitung zu Container-Labs](https://mitsurutech.co.uk/help/labs) erklären den Rest: was eine Compose-Datei enthalten darf, wie viel Speicher eine Kopie bekommt und wie eine Lehrkraft die Kopie einer lernenden Person öffnet, beobachtet und bewertet.

Noch kein Hochschulkonto? [Das erste Jahr ist kostenlos](https://mitsurutech.co.uk/pricing) für bis zu 50 Lernende, und die [Anmeldung](https://mitsurutech.co.uk/signup) braucht nur eine E-Mail-Adresse.

## Auf dem eigenen Rechner ausführen

Du brauchst Docker mit dem Compose-Plugin. Der erste Start lädt das Image herunter, rund 800 MB.

```bash
docker compose -f compose.yaml -f local.yaml up
```

Öffne <http://localhost:4040> für die Spark UI und frage in einem zweiten Terminal etwas ab:

```bash
curl -s -d "SELECT country, count(*) AS orders FROM orders GROUP BY country ORDER BY orders DESC" http://localhost:8000/sql
```

`local.yaml` veröffentlicht beide Ports nur auf `127.0.0.1`, das Lab ist also von deinem eigenen Rechner aus erreichbar und von nirgendwo sonst. Beende es mit `Ctrl-C` und entferne es mit `docker compose -f compose.yaml -f local.yaml down -v`.

## Durcharbeiten

1. **Die Daten ansehen.** `SELECT * FROM orders LIMIT 5` an `/sql` zeigt die ersten Zeilen, und `orders.dtypes` an `/df` zeigt die Typen, die Spark aus der Datei ermittelt hat.
2. **Filtern.** `orders.filter(orders.country == "UK")` an `/df` behält die UK-Bestellungen: sieben von 24.
3. **Gruppieren und zählen.** `orders.groupBy("country").count()` an `/df`: UK hat sieben Bestellungen, Japan und France sechs, Spain fünf.
4. **In SQL fragen.** `SELECT country, round(sum(quantity * unit_price), 2) AS revenue FROM orders GROUP BY country ORDER BY revenue DESC` an `/sql` ergibt Japan 717.5, UK 670.0, France 495.0 und Spain 490.0.
5. **Die Jobs ansehen.** Öffne die Spark UI und sieh dir den Reiter SQL / DataFrame an: Jede Abfrage, die du ausgeführt hast, steht dort mit ihrem Plan, und der Reiter Jobs zeigt die Arbeit, in die sie zerlegt wurde.
6. **Ein Skript ausführen.** Drei Zeilen an `/py` (`by = orders.groupBy("country").agg(F.sum("quantity").alias("units"))`, `print("countries:", by.count())` und `by.orderBy(F.desc("units"))`, jede in einer eigenen Zeile) antworten `countries: 4` und danach mit einer Tabelle, in der Japan mit 17 Stück oben steht, dann France mit 15, Spain mit 12 und UK mit 11.

## Für Lehrkräfte

Nichts in diesem Lab wird automatisch bewertet. Die lernende Person gibt ab, was sie gefunden hat, und du bewertest es, so wie jedes andere Lab.

**Verlange ein Ergebnis und einen Satz.** Eine Abfrage, die die Person selbst über `orders.csv` geschrieben hat (der Umsatz je Land, das am häufigsten bestellte Produkt, die größte Bestellung), die Tabelle, die sie ausgegeben hat, und einen Screenshot dieser Abfrage in der Spark UI, auf dem die Adresse des Labs sichtbar ist, damit klar ist, dass es ihre eigene Kopie ist. Dazu ein, zwei Sätze in eigenen Worten dazu, was Spark getan hat: Es hat die Datei gelesen, einen Plan aufgestellt und ihn als Job ausgeführt. An der Formulierung erkennst du, ob es verstanden wurde.

**Wo es in einen Kurs passt.** Das Lab passt zu Einheiten über Data Engineering und Big Data: DataFrames und SQL als zwei Wege, dieselbe Frage zu stellen, verzögerte Auswertung und Pläne, und was ein Job ist. Prüfe die Einheit deiner eigenen Prüfungsstelle auf den genauen Wortlaut; hier wird keiner zitiert.

**Wer was sieht.** Eine Kopie ist privat für die Person, die sie gestartet hat. Eine Lehrkraft des Moduls kann sie öffnen, um zu sehen, was die Person getan hat, und das Öffnen der Arbeit einer lernenden Person wird im Zugriffsprotokoll festgehalten, das die Person lesen kann.

## Was in einem Lab nicht geht

Dies ist ein einziges Spark im lokalen Modus, kein Cluster: Es gibt keinen Master und keine Worker, die man starten, stoppen oder kaputt machen könnte, und keine auf Rechner verteilten Daten. Die Daten sind eine Datei mit 24 Zeilen, Zeiten sagen also nichts über echte Lasten aus, und eine Abfrage, die weit mehr Speicher braucht, als die Kopie hat, schlägt fehl. Der Endpunkt `/df` wertet einen Python-Ausdruck aus und der Endpunkt `/py` führt ein kurzes Python-Skript aus, beide mit den Funktionen von Spark. Das ist zum Lernen der DataFrame-API in deiner eigenen privaten Kopie ohne Weg nach draußen gedacht, und keiner von beiden ist ein Dienst, den man nach außen öffnen sollte. Das Lab hat überhaupt keinen Weg ins Internet, also lässt sich nichts hineinladen, und es vergisst jede Abfrage, wenn es stoppt.

## Fehlersuche

- **Die Spark UI erscheint nicht gleich nach dem Start.** Warte ein paar Sekunden und drücke noch einmal Öffnen: Das Lab meldet sich erst bereit, wenn Spark die Datei geladen hat.
- **Eine Abfrage antwortet mit einem Fehler.** Die erste Zeile sagt, was Spark nicht verstanden hat, zum Beispiel eine Spalte, die es nicht gibt; die Spalten stehen in der Meldung.
- **Auf dem eigenen Rechner ist Port 4040 oder 8000 schon belegt.** Ändere die linke Zahl in `local.yaml` und öffne stattdessen diesen Port.

## Das Lab prüfen

`test/check.sh` startet das Lab so, wie der Runner der Plattform jeden Dienst startet (alle Capabilities entfernt, `no-new-privileges`, ein Prozesslimit, Speicherlimits und kein Weg ins Internet), und prüft dann von innerhalb seines Netzes, was diese Seite verspricht: die 24 Zeilen, den Filter, das Gruppieren und Zählen, die SQL-Antwort und die Spark UI, die die Jobs auflistet. Es braucht Docker und läuft bei jedem Push, das ist das Abzeichen oben.

```bash
test/check.sh
```

## Versionen und Lizenzen

[Apache Spark](https://spark.apache.org) steht unter der Apache-2.0-Lizenz. Das Image ist `spark:4.0.1-scala2.13-java21-python3-ubuntu`, das eigene Docker Official Image des Apache-Spark-Projekts.

Die darin laufende Anwendung hat die Lizenz ihres eigenen Projekts, oben verlinkt.

## Lizenz

Dieses Werk steht unter der Lizenz [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). Du darfst es zum Lernen und Lehren weitergeben und anpassen, wenn du Mitsuru Technologies Ltd nennst, es nicht kommerziell nutzt und alles, was du daraus machst, unter derselben Lizenz weitergibst. Der vollständige Text steht in der Datei `LICENSE`.

## Über uns

Gemacht von [Mitsuru Technologies](https://mitsurutech.co.uk) für die [MitsuruTech Learning Platform](https://mitsurutech.co.uk/learning): Python, Java, Go, Node, SQL und Linux im Browser für Schulen und Hochschulen, mit Container-Labs, aus GitHub bereitgestellten Web-Apps und Quizzen, die sich selbst bewerten. Probiere Code ohne Konto im [Playground](https://mitsurutech.co.uk/playground).

Weitere Labs vom selben Ort: [lab-juice-shop](https://github.com/mitsurutech/lab-juice-shop), [lab-dvwa](https://github.com/mitsurutech/lab-dvwa), [lab-webgoat](https://github.com/mitsurutech/lab-webgoat), [lab-mysql-phpmyadmin](https://github.com/mitsurutech/lab-mysql-phpmyadmin), [lab-git-gitea](https://github.com/mitsurutech/lab-git-gitea) und [lab-sample](https://github.com/mitsurutech/lab-sample).
