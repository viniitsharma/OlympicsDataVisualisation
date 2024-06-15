CONST = {
    "USE_DB" : "use tokyo_olympics",
    "MOST_GOLD" : "select noc, gold_count from medal AS m inner join country AS c ON m.noc_encoded = c.noc_encoded order by gold_count desc limit 1",
    "MOST_SILVER" : "select noc, silver_count from medal AS m inner join country AS c ON m.noc_encoded = c.noc_encoded order by gold_count desc limit 1",
    "MOST_BRONZE" : "select noc, bronze_count from medal AS m inner join country AS c ON m.noc_encoded = c.noc_encoded order by gold_count desc limit 1",
    "MOST_TOTAL_MEDAL" : "select noc, total_count from medal AS m inner join country AS c ON m.noc_encoded = c.noc_encoded order by gold_count desc limit 1",
    "BAR_CHART_DATA" : "select d.discipline, e.female_count, e.male_count, e.total_count from entries_gender As e inner join discipline d on e.discipline_encoding = d.discipline_encoding",
    "MAP_DATA" : "select noc, gold_count, silver_count, bronze_count, total_count from medal AS m inner join country AS c ON m.noc_encoded = c.noc_encoded"
}