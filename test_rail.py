import duckdb
con = duckdb.connect('racealpha.duckdb')
result = con.execute("""
SELECT 
  COUNT(*) as total,
  COUNT(CASE WHEN track_info ILIKE 'RAIL - True%' THEN 1 END) as true_count,
  COUNT(CASE WHEN regexp_extract(track_info, '(?:Out|[+])\\s*(\\d+\\.?\\d*)', 1) != '' THEN 1 END) as out_count,
  COUNT(CASE WHEN track_info IS NOT NULL AND track_info NOT ILIKE 'RAIL%' THEN 1 END) as non_rail,
  COUNT(CASE WHEN track_info ILIKE 'RAIL%' AND track_info NOT ILIKE 'RAIL - True%' 
    AND regexp_extract(track_info, '(?:Out|[+])\\s*(\\d+\\.?\\d*)', 1) = '' THEN 1 END) as rail_unparsed
FROM races
""").fetchone()
print(f'Total races: {result[0]}')
print(f'True rail: {result[1]} ({result[1]/result[0]*100:.1f}%)')
print(f'Out (parsed): {result[2]} ({result[2]/result[0]*100:.1f}%)')
print(f'Non-rail data: {result[3]}')
print(f'Rail but unparsed: {result[4]}')
print(f'Total parseable: {result[1]+result[2]} ({(result[1]+result[2])/result[0]*100:.1f}%)')
con.close()
