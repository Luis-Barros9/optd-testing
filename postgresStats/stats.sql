SELECT 
    -- n_distinct,
    -- null_frac,
    -- avg_width,
    -- correlation,
    -- most_common_vals,
    -- most_common_freqs
    -- histogram_bounds,
    -- most_common_elems,
    -- most_common_elem_freqs,
    -- elem_count_histogram
FROM pg_stats 
WHERE tablename = 'orders' 
  AND attname = 'o_custkey';