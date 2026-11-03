# Laboratorio de Apache Spark con Docker Compose: aprende DataFrames y Spark SQL con un archivo CSV

[English](README.md) | [Français](README.fr.md) | [日本語](README.ja.md) | [Español](README.es.md) | [Deutsch](README.de.md)

[![check](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml/badge.svg)](https://github.com/mitsurutech/lab-spark/actions/workflows/check.yml)

Un **laboratorio de Apache Spark** listo para usar en clase, de la [plataforma de aprendizaje MitsuruTech](https://mitsurutech.co.uk/es/learning), donde las escuelas y los centros dan a cada alumno sus propias bases de datos, contenedores y terminal en el navegador. Cada alumno recibe su propio Spark pequeño con un archivo CSV ya cargado como **DataFrame**: lo filtra, lo agrupa y lo cuenta, responde una pregunta en **Spark SQL** y luego abre la **Spark UI** para ver los trabajos que han generado sus consultas. Nada que instalar y ningún clúster que reservar.

Es un solo archivo `docker-compose`, `compose.yaml`, que funciona igual en tu propio equipo que en la plataforma, donde cada alumno tiene una copia privada sin salida a internet.

## Qué hay

| Servicio | Imagen | Memoria | Qué es |
| --- | --- | --- | --- |
| `spark` | [`spark:4.0.1-scala2.13-java21-python3-ubuntu`](https://hub.docker.com/_/spark) | 512 MB | Apache Spark 4.0.1 en modo local, con un pequeño servicio web delante |

El único servicio arranca Spark (el driver y dos hilos ejecutores en un solo proceso, que es lo que cabe en una copia pequeña), escribe `orders.csv` (24 pedidos con seis columnas: `order_id`, `customer`, `country`, `product`, `quantity`, `unit_price`), lo carga en un DataFrame llamado `orders` y en una tabla SQL del mismo nombre, y espera. Está sano en unos seis segundos y usa unos 380 MB en reposo, la mayor parte de sus 512 MB: los datos son diminutos, y este es un laboratorio para aprender la API, no para big data.

Responde en dos puertos, ambos dentro de la copia:

- **4040, la Spark UI.** La página del laboratorio de la plataforma la abre por ti. Sus pestañas Jobs, Stages y SQL muestran cada consulta mientras se ejecuta.
- **8000, un pequeño servicio de consultas.** Envía una consulta de Spark SQL a `/sql`, una expresión de DataFrame a `/df` o unas pocas líneas de PySpark a `/py`, y el resultado vuelve como una tabla. `/py` ejecuta el guion con `orders`, `F` y `spark` a mano y responde con lo que imprimió, y después con el valor de su última línea. Existe porque la terminal que acompaña a un laboratorio tiene `curl` pero no tiene Spark propio.

La imagen es la del propio Apache Spark, ejecutada sin modificar y fijada a una versión (nunca `latest`), de modo que el laboratorio no cambia a una clase de una lección a la siguiente. Aquí no se construye nada y nada de este repositorio se distribuye en ella. Conviene conocer un ajuste: Spark se niega a arrancar con un montón tan pequeño si no se le dice lo contrario, así que el archivo establece `spark.testing.reservedMemory`, un ajuste pensado para las pruebas del propio Spark, para que quepa en el límite de 512 MB. No lo copies a un clúster real.

## Usarlo en la plataforma

La [plataforma de aprendizaje MitsuruTech](https://mitsurutech.co.uk/es/learning) da a cada alumno su propia copia de un laboratorio, en el navegador, con una terminal al lado.

**Para probarlo:** abre la [página de contenedores](https://mitsurutech.co.uk/es/container), busca este laboratorio en **Laboratorios de ejemplo** y pulsa Iniciar. No hace falta una cuenta para probar uno.

**Para ponerlo como trabajo a una clase:**

1. En tu módulo, crea una tarea de tipo **Laboratorio** y elige **En un repositorio de GitHub**.
2. Indica `https://github.com/mitsurutech/lab-spark` con la rama `main`. El archivo compose es el predeterminado, `compose.yaml`. Haz antes un fork del repositorio si quieres cambiar el archivo o los datos.
3. Publica. La copia de cada alumno parte del commit que publicaste, así que un cambio posterior aquí no altera un laboratorio ya asignado.
4. Di en la tarea qué hay que entregar (consulta [Para profesores](#para-profesores)) y asígnala como cualquier otra tarea.

La [página de este laboratorio](https://mitsurutech.co.uk/es/help/lab-spark) del manual y su [guía de laboratorios de contenedores](https://mitsurutech.co.uk/es/help/labs) cubren el resto: qué puede decir un archivo compose, cuánta memoria recibe una copia y cómo abre, vigila y califica un profesor la copia de un alumno.

¿Aún no tienes una cuenta de centro? [El primer año es gratis](https://mitsurutech.co.uk/es/pricing) hasta 50 alumnos, y [registrarse](https://mitsurutech.co.uk/es/signup) solo pide una dirección de correo electrónico.

## Ejecutarlo en tu propio equipo

Necesitas Docker con el plugin Compose. El primer arranque descarga la imagen, que ocupa unos 800 MB.

```bash
docker compose -f compose.yaml -f local.yaml up
```

Abre <http://localhost:4040> para la Spark UI y, en otra terminal, pregúntale algo:

```bash
curl -s -d "SELECT country, count(*) AS orders FROM orders GROUP BY country ORDER BY orders DESC" http://localhost:8000/sql
```

`local.yaml` publica los dos puertos solo en `127.0.0.1`, así que el laboratorio es accesible desde tu propio ordenador y desde ningún otro sitio. Páralo con `Ctrl-C` y elimínalo con `docker compose -f compose.yaml -f local.yaml down -v`.

## Paso a paso

1. **Mira los datos.** `SELECT * FROM orders LIMIT 5` enviado a `/sql` muestra las primeras filas, y `orders.dtypes` enviado a `/df` muestra los tipos que Spark dedujo del archivo.
2. **Filtra.** `orders.filter(orders.country == "UK")` enviado a `/df` se queda con los pedidos del Reino Unido: siete de los 24.
3. **Agrupa y cuenta.** `orders.groupBy("country").count()` enviado a `/df`: el Reino Unido tiene siete pedidos, Japón y Francia seis, España cinco.
4. **Pregúntalo en SQL.** `SELECT country, round(sum(quantity * unit_price), 2) AS revenue FROM orders GROUP BY country ORDER BY revenue DESC` enviado a `/sql` da Japón 717.5, Reino Unido 670.0, Francia 495.0 y España 490.0.
5. **Mira los trabajos.** Abre la Spark UI y mira la pestaña SQL / DataFrame: cada consulta que ejecutaste está ahí, con su plan, y la pestaña Jobs tiene el trabajo en que se dividió.
6. **Ejecuta un guion.** Tres líneas enviadas a `/py` (`by = orders.groupBy("country").agg(F.sum("quantity").alias("units"))`, `print("countries:", by.count())` y `by.orderBy(F.desc("units"))`, cada una en su línea) responden `countries: 4` y luego una tabla con Japón arriba con 17 unidades, después Francia 15, España 12 y Reino Unido 11.

## Para profesores

Nada en este laboratorio se califica automáticamente. El alumno entrega lo que ha encontrado y tú lo calificas, como cualquier otro laboratorio.

**Pide un resultado y una frase.** Una consulta que el propio alumno haya escrito sobre `orders.csv` (los ingresos por país, el producto más pedido, el pedido más grande), la tabla que imprimió y una captura de pantalla de esa consulta en la Spark UI con la dirección propia del laboratorio visible, para que quede claro que es su copia. Después una o dos frases, con sus propias palabras, sobre lo que hizo Spark: leyó el archivo, construyó un plan y lo ejecutó como un trabajo. La redacción te dice si lo ha entendido.

**Dónde encaja en un curso.** El laboratorio encaja en unidades de ingeniería de datos y big data: los DataFrames frente a SQL como dos formas de hacer una misma pregunta, la evaluación perezosa y los planes, y qué es un trabajo. Consulta la unidad de tu propio organismo de certificación para ver la redacción exacta; aquí no se cita ninguna.

**Quién ve qué.** Una copia es privada del alumno que la inició. Un miembro del personal que da el módulo puede abrirla para ver lo que ha hecho el alumno, y abrir el trabajo de un alumno queda anotado en el registro de accesos, que el alumno puede leer.

## Lo que no funciona en un laboratorio

Es un solo Spark en modo local, no un clúster: no hay un maestro ni un worker que iniciar, parar o romper, ni datos repartidos entre máquinas. Los datos son un archivo de 24 filas, así que los tiempos no dicen nada de cargas reales, y una consulta que necesite mucha más memoria de la que tiene la copia fallará. El punto de acceso `/df` evalúa una expresión de Python y el punto de acceso `/py` ejecuta un guion corto de Python, ambos con las funciones de Spark, lo que sirve para aprender la API de DataFrame en tu propia copia privada, sin salida al exterior, y ninguno de los dos es un servicio que se deba exponer. El laboratorio no tiene ninguna salida a internet, así que no se puede descargar nada en él, y olvida cada consulta cuando se detiene.

## Solución de problemas

- **La Spark UI no aparece justo después de Iniciar.** Espera unos segundos y pulsa Abrir de nuevo: el laboratorio solo se declara listo cuando Spark ha cargado el archivo.
- **Una consulta responde con un error.** La primera línea dice lo que Spark no entendió, por ejemplo una columna que no existe; las columnas se listan en el mensaje.
- **En tu propio equipo, el puerto 4040 o el 8000 ya está en uso.** Cambia el número de la izquierda en `local.yaml` y abre ese puerto en su lugar.

## Comprobar el laboratorio

`test/check.sh` arranca el laboratorio como lo hace el ejecutor de la plataforma con cada servicio (todas las capacidades retiradas, `no-new-privileges`, un límite de procesos, límites de memoria y sin salida a internet) y luego comprueba desde dentro de su red lo que promete esta página: las 24 filas, el filtro, la agrupación y el recuento, la respuesta SQL y la Spark UI listando los trabajos. Necesita Docker y se ejecuta en cada push, que es la insignia de la parte superior.

```bash
test/check.sh
```

## Versiones y licencias

[Apache Spark](https://spark.apache.org) tiene licencia Apache-2.0. La imagen es `spark:4.0.1-scala2.13-java21-python3-ubuntu`, la imagen oficial de Docker del propio proyecto Apache Spark.

La aplicación que contiene tiene la licencia de su propio proyecto, enlazada arriba.

## Licencia

Esta obra está bajo licencia [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). Puedes compartirla y adaptarla para aprender y enseñar, si citas a Mitsuru Technologies Ltd, no la usas comercialmente y compartes lo que hagas con ella bajo la misma licencia. El texto completo está en el archivo `LICENSE`.

## Acerca de

Hecho por [Mitsuru Technologies](https://mitsurutech.co.uk) para la [plataforma de aprendizaje MitsuruTech](https://mitsurutech.co.uk/es/learning): Python, Java, Go, Node, SQL y Linux en el navegador para escuelas y centros, con laboratorios de contenedores, aplicaciones web desplegadas desde GitHub y cuestionarios que se califican solos. Prueba código sin cuenta en el [entorno de pruebas](https://mitsurutech.co.uk/es/playground).

Más laboratorios del mismo sitio: [lab-juice-shop](https://github.com/mitsurutech/lab-juice-shop), [lab-dvwa](https://github.com/mitsurutech/lab-dvwa), [lab-webgoat](https://github.com/mitsurutech/lab-webgoat), [lab-mysql-phpmyadmin](https://github.com/mitsurutech/lab-mysql-phpmyadmin), [lab-git-gitea](https://github.com/mitsurutech/lab-git-gitea) y [lab-sample](https://github.com/mitsurutech/lab-sample).
