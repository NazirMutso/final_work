# Итоговая работа "Python для ETL"
___
## Установка
python 3.10

requirements.txt (список использованных модулей)
___
## Конфигурация и настройка
Установка библиотек через [Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#special-case-adding-dependencies-via-requirements-txt-file) \
Использовано логгирование по умолчанию, поскольку AirFlow записывает в логи все действия.
Настроен отлов ошибок и исключений прерывающих работу программы.
___
## Допущения
1. Программа выполняется только с помощью Python кода, посредством декоратора "task" из библиотеки "airflow.decorators".
2. Не стал делить код на файлы, посчитал что программа небольшая.
3. Использовал импортирование библиотек в тасках согласно [документации airflow](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
4. В таске 'get_companies_61' не очень хороший код по моему, делал вариант с фильтром данных с файла за раз, но это занимало в 2 раза больше времени чем ранее реализованный способ построчной обработки. Возможная причина в железе.
5. В каждом таске есть проверка на наличие ключевых параметров (файл в репозитории, данные в БД), чтобы не надо было перезапускать таски.
6. Запуск параллельного исполнения тасков парсинга и получение компаний с ОКВЭД 61 сильно нагружает ПК. Возможно лучше сделать очередь.
7. Создал БД в папке DAG, не стал подключаться к существующей.
___
## Задание
Разработать поток работ для Apache AirFlow, включающий этапы:\
1. Загрузка архива ЕГРЮЛ;
2. Выбор компаний телеком компаний (ОКВЭД 61) из архива ЕГРЮЛ;
3. Поиск вакансий "Middle Python Developer" с hh.ru
4. Выбор вакансий телеком компаний и вывод топ 10 ключевых навыков указанных в вакансиях.