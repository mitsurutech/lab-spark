# Lab Apache Spark avec Docker Compose : apprendre les DataFrames et Spark SQL sur un fichier CSV

[English](README.md) | [Français](README.fr.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Deutsch](README.de.md)

[![check](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml/badge.svg)](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml)

Un **lab Apache Spark** prêt à l’emploi pour la classe, issu de la [plateforme d’apprentissage MitsuruTech](https://mitsurutech.co.uk/fr/learning), où les écoles et les établissements donnent à chaque élève ses propres bases de données, conteneurs et terminal dans le navigateur. Chaque élève reçoit son propre petit Spark avec un fichier CSV déjà chargé sous forme de **DataFrame** : il le filtre, le regroupe et le compte, répond à une question en **Spark SQL**, puis ouvre l’**interface Spark UI** pour voir les jobs que ses requêtes ont produits. Rien à installer, et aucun cluster à réserver.

Tout tient dans un seul fichier `docker-compose`, `compose.yaml`, qui fonctionne de la même façon sur votre propre machine et sur la plateforme, où chaque élève obtient une copie privée sans aucun accès à internet.

## Ce que vous y trouvez

| Service | Image | Mémoire | Ce que c’est |
| --- | --- | --- | --- |
| `spark` | [`spark:4.0.1-scala2.13-java21-python3-ubuntu`](https://hub.docker.com/_/spark) | 512 Mo | Apache Spark 4.0.1 en mode local, avec un petit service web devant lui |

Le service unique démarre Spark (le driver et deux threads d’exécution dans un seul processus, ce qui tient dans une petite copie), écrit `orders.csv` (24 commandes avec six colonnes : `order_id`, `customer`, `country`, `product`, `quantity`, `unit_price`), le charge dans un DataFrame appelé `orders` et dans une table SQL du même nom, puis attend. Il est opérationnel en six secondes environ et utilise à peu près 380 Mo au repos, soit l’essentiel de ses 512 Mo : les données sont minuscules, et c’est un lab pour apprendre l’API, pas pour le big data.

Il répond sur deux ports, tous deux à l’intérieur de la copie :

- **4040, Spark UI.** La page du lab de la plateforme l’ouvre pour vous. Ses onglets Jobs, Stages et SQL montrent chaque requête pendant son exécution.
- **8000, un petit service de requêtes.** Envoyez une requête Spark SQL à `/sql`, une expression DataFrame à `/df` ou quelques lignes de PySpark à `/py`, et le résultat revient sous forme de tableau. `/py` exécute le script avec `orders`, `F` et `spark` à portée de main et répond avec ce qu’il a affiché, puis avec la valeur de sa dernière ligne. Il existe parce que le terminal qui accompagne un lab a `curl` mais pas de Spark à lui.

L’image est celle d’Apache Spark lui-même, exécutée sans modification et figée sur une version (jamais `latest`), de sorte que le lab ne change pas sous les pieds d’une classe d’une leçon à l’autre. Rien n’est construit ici et rien de ce dépôt n’y est distribué. Un réglage mérite d’être connu : Spark refuse de démarrer avec un tas aussi petit sauf indication contraire, le fichier fixe donc `spark.testing.reservedMemory`, un réglage prévu pour les propres tests de Spark, pour tenir dans la limite de 512 Mo. Ne le copiez pas sur un vrai cluster.

## L’utiliser sur la plateforme

La [plateforme d’apprentissage MitsuruTech](https://mitsurutech.co.uk/fr/learning) donne à chaque élève sa propre copie d’un lab, dans le navigateur, avec un terminal à côté.

**Pour l’essayer :** ouvrez la [page des conteneurs](https://mitsurutech.co.uk/fr/container), trouvez ce lab dans **Laboratoires d’exemple** et appuyez sur Démarrer. Aucun compte n’est nécessaire pour en essayer un.

**Pour le donner comme travail à une classe :**

1. Dans votre module, créez une tâche de type **Lab** et choisissez **Dans un dépôt GitHub**.
2. Indiquez `https://github.com/mitsurutech/lab-spark` avec la branche `main`. Le fichier compose est celui par défaut, `compose.yaml`. Faites d’abord un fork du dépôt si vous voulez modifier le fichier ou les données.
3. Publiez. La copie de chaque élève part du commit que vous avez publié, de sorte qu’une modification ultérieure ici ne change pas un lab déjà donné.
4. Précisez dans la tâche ce qu’il faut rendre (voir [Pour les enseignants](#pour-les-enseignants)), puis distribuez-la comme n’importe quelle autre tâche.

La [page de ce lab](https://mitsurutech.co.uk/fr/help/lab-spark) et le [guide des labs de conteneurs](https://mitsurutech.co.uk/fr/help/labs) du manuel couvrent le reste : ce qu’un fichier compose peut contenir, la mémoire dont dispose une copie, et comment un enseignant ouvre, suit et note la copie d’un élève.

Pas encore de compte d’établissement ? [La première année est gratuite](https://mitsurutech.co.uk/fr/pricing) jusqu’à 50 élèves, et [l’inscription](https://mitsurutech.co.uk/fr/signup) ne demande qu’une adresse e-mail.

## L’exécuter sur votre propre machine

Il vous faut Docker avec le plugin Compose. Le premier démarrage télécharge l’image, qui pèse environ 800 Mo.

```bash
docker compose -f compose.yaml -f local.yaml up
```

Ouvrez <http://localhost:4040> pour Spark UI, et dans un autre terminal posez-lui une question :

```bash
curl -s -d "SELECT country, count(*) AS orders FROM orders GROUP BY country ORDER BY orders DESC" http://localhost:8000/sql
```

`local.yaml` publie les deux ports sur `127.0.0.1` uniquement, de sorte que le lab n’est accessible que depuis votre ordinateur. Arrêtez-le avec `Ctrl-C` et supprimez-le avec `docker compose -f compose.yaml -f local.yaml down -v`.

## Pas à pas

1. **Regardez les données.** `SELECT * FROM orders LIMIT 5` envoyé à `/sql` montre les premières lignes, et `orders.dtypes` envoyé à `/df` montre les types que Spark a déduits du fichier.
2. **Filtrez.** `orders.filter(orders.country == "UK")` envoyé à `/df` garde les commandes du Royaume-Uni : sept sur 24.
3. **Regroupez et comptez.** `orders.groupBy("country").count()` envoyé à `/df` : le Royaume-Uni compte sept commandes, le Japon et la France six, l’Espagne cinq.
4. **Posez la question en SQL.** `SELECT country, round(sum(quantity * unit_price), 2) AS revenue FROM orders GROUP BY country ORDER BY revenue DESC` envoyé à `/sql` donne Japon 717.5, Royaume-Uni 670.0, France 495.0 et Espagne 490.0.
5. **Voyez les jobs.** Ouvrez Spark UI et regardez l’onglet SQL / DataFrame : chaque requête que vous avez lancée s’y trouve, avec son plan, et l’onglet Jobs montre les morceaux de travail en lesquels elle a été découpée.
6. **Lancez un script.** Trois lignes envoyées à `/py` (`by = orders.groupBy("country").agg(F.sum("quantity").alias("units"))`, `print("countries:", by.count())` et `by.orderBy(F.desc("units"))`, chacune sur sa ligne) répondent `countries: 4`, puis un tableau avec le Japon en tête à 17 unités, puis la France 15, l’Espagne 12 et le Royaume-Uni 11.

## Pour les enseignants

Rien dans ce lab n’est noté automatiquement. L’élève rend ce qu’il a trouvé et vous le notez, comme pour n’importe quel autre lab.

**Demandez un résultat et une phrase.** Une requête que l’élève a écrite lui-même sur `orders.csv` (le chiffre d’affaires par pays, le produit le plus commandé, la plus grosse commande), le tableau qu’elle a affiché, et une capture d’écran de cette requête dans Spark UI avec l’adresse propre au lab visible, pour que ce soit clairement sa copie. Puis une phrase ou deux, dans ses propres mots, sur ce que Spark a fait : il a lu le fichier, construit un plan et l’a exécuté sous forme de job. La formulation montre si c’est compris.

**Sa place dans un cursus.** Le lab convient aux unités d’ingénierie des données et de big data : les DataFrames face à SQL comme deux façons de poser une même question, l’évaluation paresseuse et les plans, et ce qu’est un job. Vérifiez l’unité de votre propre organisme de certification pour la formulation exacte ; aucune n’est citée ici.

**Qui voit quoi.** Une copie est privée pour l’élève qui l’a démarrée. Un membre du personnel qui enseigne le module peut l’ouvrir pour voir ce que l’élève a fait, et l’ouverture du travail d’un élève est inscrite dans le journal d’accès, que l’élève peut lire.

## Ce qui ne fonctionne pas dans un lab

C’est un seul Spark en mode local, pas un cluster : il n’y a ni maître ni worker à démarrer, arrêter ou casser, et pas de données réparties sur des machines. Les données tiennent dans un fichier de 24 lignes, les temps mesurés ne disent donc rien des charges réelles, et une requête qui demande bien plus de mémoire que n’en a la copie échouera. Le point d’accès `/df` évalue une expression Python et le point d’accès `/py` exécute un court script Python, tous deux avec les fonctions de Spark, ce qui sert à apprendre l’API DataFrame dans votre propre copie privée, sans accès à l’extérieur, et ni l’un ni l’autre n’est un service à exposer. Le lab n’a aucun accès à internet, rien ne peut donc y être téléchargé, et il oublie chaque requête quand il s’arrête.

## Dépannage

- **Spark UI n’apparaît pas tout de suite après Démarrer.** Attendez quelques secondes et appuyez de nouveau sur Ouvrir : le lab ne se déclare prêt que lorsque Spark a chargé le fichier.
- **Une requête répond par une erreur.** La première ligne dit ce que Spark n’a pas compris, par exemple une colonne qui n’existe pas ; les colonnes sont listées dans le message.
- **Sur votre propre machine, le port 4040 ou 8000 est déjà utilisé.** Changez le nombre de gauche dans `local.yaml` et ouvrez ce port à la place.

## Vérifier le lab

`test/check.sh` démarre le lab comme le fait l’exécuteur de la plateforme pour chaque service (toutes les capacités retirées, `no-new-privileges`, une limite de processus, des limites de mémoire et aucun accès à internet), puis vérifie depuis l’intérieur de son réseau ce que cette page promet : les 24 lignes, le filtre, le regroupement et le comptage, la réponse SQL, Spark UI qui liste les jobs. Il demande Docker et s’exécute à chaque push, ce qui correspond au badge en haut de la page.

```bash
test/check.sh
```

## Versions et licences

[Apache Spark](https://spark.apache.org) est sous licence Apache-2.0. L’image est `spark:4.0.1-scala2.13-java21-python3-ubuntu`, l’image Docker officielle du projet Apache Spark.

L’application qu’elle contient a la licence de son propre projet, indiquée ci-dessus.

## Licence

Cette œuvre est sous licence [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). Vous pouvez la partager et l’adapter pour apprendre et enseigner, à condition de citer Mitsuru Technologies Ltd, de ne pas l’utiliser commercialement et de partager tout ce que vous en tirez sous la même licence. Le texte complet est dans le fichier `LICENSE`.

## À propos

Réalisé par [Mitsuru Technologies](https://mitsurutech.co.uk) pour la [plateforme d’apprentissage MitsuruTech](https://mitsurutech.co.uk/fr/learning) : Python, Java, Go, Node, SQL et Linux dans le navigateur pour les écoles et les établissements, avec des labs de conteneurs, des applications web déployées depuis GitHub et des quiz qui se corrigent seuls. Essayez du code sans compte dans le [bac à sable](https://mitsurutech.co.uk/fr/playground).

D’autres labs du même éditeur : [lab-juice-shop](https://github.com/mitsurutech/lab-juice-shop), [lab-dvwa](https://github.com/mitsurutech/lab-dvwa), [lab-webgoat](https://github.com/mitsurutech/lab-webgoat), [lab-mysql-phpmyadmin](https://github.com/mitsurutech/lab-mysql-phpmyadmin), [lab-git-gitea](https://github.com/mitsurutech/lab-git-gitea) et [lab-sample](https://github.com/mitsurutech/lab-sample).
