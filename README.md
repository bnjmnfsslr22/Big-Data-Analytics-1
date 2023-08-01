# Big-Data-Analytics-1-
Dieses Repository enthält eine Reihe von Python-Programmen, die für verschiedene Big-Data-Analytics-Aufgaben erstellt wurden.

# Inhaltsverzeichnis
1. Project-1
2. Project-2
3. prjoect-3
4. project-4


# Project-1
# Big Data Analytics - Map-Reduce Zahlenzähler
Dieses Repository beinhaltet das Projekt "Map-Reduce Zahlenzähler", implementiert in Python. 
Das Projekt liest eine große CSV-Datei ein und zählt die Häufigkeit der Zahlen innerhalb dieser Datei. Außerdem wird ein Histogramm der Zahlenhäufigkeiten erstellt, 
bei dem die Zahlen auf der x-Achse nach ihrer Größe angeordnet sind.

# Setup
Das Projekt wurde in Jupyter Notebook erstellt und kann in dieser Umgebung ausgeführt werden.

# Benötigte Bibliotheken:
- csv
- functools
- matplotlib

# Struktur und Funktionsweise des Projekts
# CSV-Datei einlesen: 
Die CSV-Datei wird geöffnet und zeilenweise eingelesen. Jede Zeile wird mit map in Integer-Werte konvertiert und der Liste "zahlen" hinzugefügt.
# Reducer-Funktion:
Die Reducer-Funktion wird verwendet, um die Häufigkeiten der Zahlen in der Liste "zahlen" zu zählen. Sie nimmt zwei Dictionaries als Eingabe, die jeweils die Häufigkeit einer Zahl in der Liste repräsentieren und kombiniert diese.
# Funktion f: 
Die Funktion f erstellt für jede eingelesene Zahl ein Dictionary mit dem Schlüssel als Zahl und dem Wert 1. Dieses Dictionary repräsentiert das Vorkommen der jeweiligen Zahl in der Liste.
# Anwendung der F-Funktion auf die Liste "zahlen": 
Die Funktion "f" wird auf die gesamte Liste "zahlen" angewendet und gibt eine Liste von Dictionaries zurück.
# Berechnung der tatsächlichen Häufigkeit der Zahlen: 
Die reduce Funktion wird verwendet, um ein Dictionary zu erstellen, das das Vorkommen jeder Zahl in der Liste "zahlen" enthält.
# Ausgabe der sortierten Zahlen: 
Eine zusätzliche Funktion gibt die sortierten Zahlen und ihre Häufigkeiten aus.
# Visualisierung der Ergebnisse: 
Mit Matplotlib wird ein Histogramm der Zahlenhäufigkeiten erstellt.

# Verwendung
Das Python-Skript ist darauf ausgelegt, mit einer spezifischen CSV-Datei zu arbeiten. Ändern Sie den file_path entsprechend ab, um eine andere Datei zu verwenden.

# Project-2
# Abfragen mit Apache Drill
# Die Aufgabe bestand darin, sich mit Apache Drill vertraut zu machen und bestimmte Aufgaben mithilfe von SQL-Abfragen zu lösen.

# Werkzeuge
Die wichtigsten Werkzeuge, die bei diesem Projekt eingesetzt wurden, sind:
Apache Drill: Ein leistungsfähiges Open-Source-Framework, das SQL-Abfragen auf nicht relationale Datenstrukturen ermöglicht.
Jupyter Notebook: Ein webbasiertes interaktives Computationsumfeld, das verwendet wurde, um die SQL-Abfragen auszuführen und die Ergebnisse zu dokumentieren.
PyODBC: Eine Python-Bibliothek, die den Zugriff auf Datenbanken über ODBC-Treiber ermöglicht.

# Datensätze
Die Analyse bezieht sich auf verschiedene Datensätze, darunter:
- Sensordaten in einem Tab-separated-Value-Format (co2data.tsv), das unter dfs.data.co2data.tsv in Apache Drill konfiguriert ist.
- Daten von der bdapi, eine HTTP-Datenquelle mit Sensornamen und -Positionen (bdapi.labels), eine Liste von JSON-Objekten mit Sensordaten (bdapi.sensors), und ein JSON-Objekt, das dynamische Sensordaten anbietet (bdapi.sensorslastday).

# Hauptaufgaben
Die Aufgaben, die in diesem Projekt ausgeführt wurden, sind:
- Untersuchung der Sensordaten in dfs.data.co2data.tsv, einschließlich Zählen der Sensoren, Bestimmung der Datenpunkte pro Sensor, Vorbereitung der Werte für sinnvolle Datentypen und Analyse der Temperatur- und CO2-Werte.
- Verknüpfung der Daten aus dfs.data.co2data.tsv mit den Daten aus bdapi.labels, Anzeige von Sensornamen und Besitzer zu jeder Seriennummer, Erstellung einer View und Anzeige aller Sensoren eines bestimmten Besitzers.

Alle Aufgaben wurden in SQL ausgeführt und mit Python und PyODBC verarbeitet.

# Setup und Ausführung
Um dieses Projekt nachzuvollziehen, sollten Sie zuerst den Ordner work/drill-driver in Ihrem JupyterLab Workspace erstellen und eine neue Datei odbc.ini dort anlegen. Ein Beispiel dafür finden Sie unter Big_Data_Analytics_1/public/drill-driver/.
Sie können dann das Notebook BDA1_A2_Drill öffnen und die Zellen ausführen, um die SQL-Abfragen auszuführen und die Ergebnisse zu sehen.
# Ergebnisse
Die Ergebnisse dieser Aufgabe wurden in Form von gedruckten Ausgaben der SQL-Abfragen im Jupyter Notebook dargestellt. Diese umfassen Statistiken über die Anzahl der Sensoren und Datenpunkte pro Sensor, Umrechnung der Daten in sinnvolle Datentypen, sowie Analyse von Temperatur- und CO2-Werten. Darüber hinaus wurden Sensordaten mit Sensornamen und Besitzerinformationen verknüpft und weitere Analysen durchgeführt.
Weitere Details
Für eine detaillierte Beschreibung der Aufgaben und ihrer Lösungen, einschließlich der spezifischen SQL-Abfragen, die verwendet wurden, und der erzielten Ergebnisse, siehe die vollständige Dokumentation im Jupyter Notebook BDA1_A2_Drill.

# Project-3
# Aufgabe 3: Abfragen mit Apache Spark

Dieses Praktikum führt Sie in die Arbeit mit Apache Spark ein. Es erfordert, dass Sie die bereitgestellten Spark RDDs entsprechend transformieren, um verschiedene Abfragen und Analysen durchzuführen.
Arbeitsanweisung
Datei: Nutzen Sie das vorliegende Jupyter Notebook BDA1_A3_Spark.ipynb für Ihre Lösungen. Das Notebook finden Sie im Verzeichnis des Repositories.
Code: In den markierten Zellen muss ausführbarer Python-Code vorliegen.
Ausgabe: Die Ausgabe soll unterhalb der jeweiligen Zellen produziert werden.
Dokumentation: Liefern Sie auch aussagekräftiges Markdown zu Ihrem Code (Vorgehen, Quellen, etc) ab.

# Hinweise zur Vorbereitung

    Spark Master: Verwenden Sie immer den vorgegeben Spark Master, um Inkonsistenzen der Python3-Versionen zwischen Worker und Client zu verhindern.
    SparkContext Konfiguration: Ändern Sie nicht die SparkContext-Konfiguration und beenden Sie bitte den SparkContext nachdem Sie die Bearbeitung beenden, um die Ressourcen wieder freizugeben! (Verwendung der stop_sc1() Funktion).
    Installation: Stellen Sie sicher, dass pyspark installiert ist (pip install pyspark).

# Datenquellen
Für diese Aufgabe steht ein HDFS mit dem Namenode namenode unter Port 19000 bereit. Die Dateien darin:
/data/bda1/co2data.tsv: Datensatz von Messungen verschiedener CO2-Sensoren.
/data/bda1/co2data_pm.tsv: Datensatz von Messungen verschiedener CO2-Sensoren mit Partikelmessung. pm steht für Partikeldichte, npm für die Partikelanzahl bestimmter Größen.

# Aufgabe 3a: Untersuchen der Sensordaten

Sie müssen die folgenden Fragen beantworten, indem Sie jeweils geeignete RDD-Operationen in Spark ausführen:

    Anzahl der Messungen in den beiden Datenmengen.
    Ausgabe der Attributnamen.
    Anzahl der verschiedenen Sensoren (angegeben im Feld serial_number).
    Anzahl der Datenpunkte je Sensor.
    Höchster und niedrigster Temperaturwert.
    Durchschnittlicher CO2-Wert je Sensor (auf drei Nachkommastellen gerundet).

# Aufgabe 3b: Analyse der Datenmengen

Nach der Analyse der Daten mit Spark sollten Sie Aussagen über die beiden Datenmengen treffen. Zum Beispiel, ob dieselben Sensoren in beiden Datenmengen vorkommen.

#  Spark Context erzeugen

Verwenden Sie die im Notebook bereitgestellten Code-Schnipsel, um den Spark Context zu erzeugen.

# Project-4
