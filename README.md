# Big Data Analytics

### Dieses Repository enthält vier Hauptprojekte, die verschiedene Aspekte der Big Data Analyse adressieren. Jedes Projekt verwendet Python und spezielle Big Data Tools und Bibliotheken.
## Inhaltsverzeichnis

    Projekt 1: Map-Reduce Zahlenzähler
    Projekt 2: Abfragen mit Apache Drill
    Projekt 3: Abfragen mit Apache Spark
    Projekt 4: DataFrames mit dem Iris-Datensatz

# Projekt 1: Map-Reduce Zahlenzähler

Dieses Projekt verwendet den Map-Reduce Ansatz, um die Häufigkeit von Zahlen in einer großen CSV-Datei zu zählen und visualisiert die Ergebnisse in einem Histogramm.
## Setup

Um das Projekt auszuführen, benötigen Sie ein Jupyter Notebook und die folgenden Python-Bibliotheken: csv, functools und matplotlib.
## Struktur und Funktionsweise

Dieses Projekt implementiert die Map-Reduce Logik, um eine große CSV-Datei zu analysieren, die Häufigkeit jeder Zahl zu zählen und die Ergebnisse zu visualisieren.
## Ausführung

Ändern Sie den Dateipfad entsprechend, um eine andere Datei zu verwenden.
# Projekt 2: Abfragen mit Apache Drill

Dieses Projekt setzt Apache Drill ein, um SQL-Abfragen auf verschiedene Datensätze auszuführen und die Ergebnisse zu analysieren.
## Setup

Sie benötigen Apache Drill, Jupyter Notebook und die PyODBC Python-Bibliothek. Der Ordner work/drill-driver muss in Ihrem JupyterLab Workspace erstellt werden, und eine neue Datei odbc.ini muss dort angelegt werden.
## Struktur und Funktionsweise

Das Projekt verwendet Apache Drill, um SQL-Abfragen auf verschiedene Datensätze auszuführen und die Ergebnisse mit Python und PyODBC zu verarbeiten.
## Ausführung

Das Jupyter Notebook BDA1_A2_Drill enthält die SQL-Abfragen und zeigt die Ergebnisse an.
# Projekt 3: Abfragen mit Apache Spark

Dieses Projekt führt Sie in die Arbeit mit Apache Spark ein und verlangt die Durchführung verschiedener Datenanalysen mittels Spark RDDs.
## Setup

Sie müssen sicherstellen, dass pyspark installiert ist und der vorgegebene Spark Master verwendet wird. Der SparkContext sollte am Ende der Bearbeitung beendet werden, um die Ressourcen wieder freizugeben.
## Struktur und Funktionsweise

Die Aufgaben des Projekts umfassen die Analyse von Sensordaten und die Beantwortung verschiedener Fragen durch geeignete RDD-Operationen in Spark.
## Ausführung

Verwenden Sie das Jupyter Notebook BDA1_A3_Spark für Ihre Lösungen und erzeugen Sie den Spark Context mit dem bereitgestellten Code-Schnipsel.
# Projekt 4: DataFrames mit dem Iris-Datensatz

Dieses Projekt behandelt das Einlesen von Daten in einen DataFrame, die Erstellung grundlegender Statistiken und die visuelle Darstellung des Iris-Datensatzes.
## Setup

Das Projekt ist in Python geschrieben und verwendet die Bibliotheken Pandas, Matplotlib und Seaborn.
## Struktur und Funktionsweise

Das Projekt besteht aus verschiedenen Aufgaben, die sich mit dem Einlesen des Iris-Datensatzes in einen DataFrame, dem Laden von Daten, dem Erzeugen von Statistiken und Visualisierungen, sowie der Ableitung von Schlussfolgerungen für eine mögliche Klassifikation befassen.
## Ausführung

Das Jupyter Notebook enthält ausführbaren Code und Markdown-Kommentare zur Erläuterung. Sie können den Code direkt im Jupyter Notebook ausführen
