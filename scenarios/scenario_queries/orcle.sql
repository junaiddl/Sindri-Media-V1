 CREATE OR REPLACE EDITIONABLE PROCEDURE "BERT"."SP_RFMS_EXPORT_OVERHEAD" (

    p_fy in number,

    p_fund_center in varchar2,

    p_status_id in number,

    p_is_budget_officer in char,    --  Used for "Budget Officer" version

    resultset out sys_refcursor,    --  Overhead Rate

    resultset2 out sys_refcursor,   --  Overhead Accounts

    resultset3 out sys_refcursor,   --  Overhead Allocation

    resultset4 out sys_refcursor    --  Other Accounts

)

AS

 

p_new_status_id number := p_status_id;

 

BEGIN

  if ( ( p_status_id is NULL ) or ( p_fy < bert.fcn_wcf_current_fy() ) ) then

    p_new_status_id := 3;

  end if;

 

    --  Overhead Rate

    OPEN resultset FOR   

    select sum( case when a.billing_type = 'O' then a.bfy_estimate else 0 end ) as requirement_by

    , sum( case when a.billing_type = 'O' then 0 else a.bfy_estimate end ) as base_by

    , round( case when sum( case when a.billing_type = 'O' then 0 else a.bfy_estimate end ) = 0 then 0 else ( ( ( sum( case when a.billing_type = 'O' then a.bfy_estimate else 0 end ) ) / ( sum( case when a.billing_type = 'O' then 0 else a.bfy_estimate end ) ) ) ) end , 7 ) as rate_by

    , sum( case when a.billing_type = 'O' then a.cfy_estimate else 0 end ) as requirement_cy

    , sum( case when a.billing_type = 'O' then 0 else a.cfy_estimate end ) as base_cy

    , round( case when sum( case when a.billing_type = 'O' then 0 else a.cfy_estimate end ) = 0 then 0 else ( ( ( sum( case when a.billing_type = 'O' then a.cfy_estimate else 0 end ) ) / ( sum( case when a.billing_type = 'O' then 0 else a.cfy_estimate end ) ) ) ) end , 7 ) as rate_cy

 

--    select a.*

    from bert.v_rfms_budget_history_grouped a

    left join (

      select b.fiscal_year

      , b.account_no

      , max( b.status_id ) as status_id

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.status_id <= nvl( p_new_status_id, 3 )

--      and b.status_id <= 1

 

      group by b.fiscal_year

      , b.account_no

    ) c

    on a.fiscal_year = c.fiscal_year

    and a.account_no = c.account_no

    and a.status_id = c.status_id

 

    where 1 = 1

    and a.count_in_boc_sum = '1'

    and a.passthrough = '0'

    and a.fiscal_year = p_fy

--    and a.fiscal_year = 2025

 

    and a.fund_center_overhead in (

      select distinct b.fund_center_overhead

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.rnk_change = 1

      and b.passthrough = '0'

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.fund_center like p_fund_center || '%'

--      and b.fund_center like 'DL900' || '%'

    )

 

    and ( 1 = 2

      or ( ( a.status_id_current = 3 ) and ( a.status_id = a.status_id_current ) )

      or ( ( a.status_id_current != 3 ) and ( a.status_id <= nvl( p_new_status_id, 3 ) ) and ( c.account_no is NOT NULL ) )

--      or ( 1 = 1 )

    )

 

    and ( 1 = 2

      or ( coalesce( p_is_budget_officer, '0' ) = '1' and a.is_budget_officer = 'Y' )

      or ( coalesce( p_is_budget_officer, '0' ) = '0' and a.is_budget_officer = 'N' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'Y' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'N' )

    )

    ;

 

    --  output Overhead Accounts

    OPEN resultset2 FOR

    select coalesce( a.service_provider_office, 'TOTAL' ) as office

    , a.billing_type_title

    , a.account_no

    , a.service_title as account_title

    , sum( a.pres_bud ) as pres_bud

    , sum( a.adjustment ) as adjustments

    , sum( a.cfy_estimate ) as current_estimate

    , sum( a.fixed_cost_calc ) as fixed_costs

    , sum( a.baseline_change ) as baseline_change

    , sum( a.program_change_1 ) as program_change_1

    , sum( a.program_change_2 ) as program_change_2

    , sum( a.program_change_3 ) as program_change_3

    , sum( a.bfy_estimate ) as estimate

 

    --    select a.*

    from bert.v_rfms_budget_history_grouped a

    left join (

      select b.fiscal_year

      , b.account_no

      , max( b.status_id ) as status_id

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.status_id <= nvl( p_new_status_id, 3 )

--      and b.status_id <= 1

 

      group by b.fiscal_year

      , b.account_no

    ) c

    on a.fiscal_year = c.fiscal_year

    and a.account_no = c.account_no

    and a.status_id = c.status_id

 

    where 1 = 1

    and a.billing_type = 'O'

    and a.count_in_boc_sum = '1'

    and a.passthrough = '0'

    and a.fiscal_year = p_fy

--    and a.fiscal_year = 2025

 

    and ( a.fund_center_overhead in coalesce( (

        select distinct b.fund_center_overhead

 

        from bert.v_rfms_accounts_grouped b

        where 1 = 1

        and b.rnk_change = 1

        and b.billing_type = 'O'

        and b.passthrough = '0'

        and b.fiscal_year = p_fy

--        and b.fiscal_year = 2025

        and b.fund_center like p_fund_center || '%'

--        and b.fund_center like 'DS621' || '%'

      ), 'DSOVR' ) )

 

      --added 9/26 so the tab match the fc report numbers

         and ( 1 = 2

      or ( ( a.status_id_current = 3 ) and ( a.status_id = a.status_id_current ) )

      or ( ( a.status_id_current != 3 ) and ( a.status_id <= nvl( p_new_status_id, 3 ) ) and ( c.account_no is NOT NULL ) )

--      or ( 1 = 1 )

    )

       and ( 1 = 2

      or ( coalesce( '1', '0' ) = '1' and a.is_budget_officer = 'Y' )

      or ( coalesce( '1', '0' ) = '0' and a.is_budget_officer = 'N' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'Y' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'N' )

    )

 

    group by grouping sets ( (), ( a.service_provider_office

      , a.billing_type_title

      , a.account_no

      , a.service_title

    ) )

 

    order by a.account_no NULLS FIRST

    ;

 

--  output Overhead Allocation

    OPEN resultset3 FOR

    select coalesce( a.office, 'TOTAL' ) as office

    , a.billing_type_title

    , a.passthrough_title

    , a.account_no

    , a.account_title

    , sum( base_wo_overhead ) as base_wo_overhead

    , sum( a.adjustment ) as adjustment

    , sum( a.cfy_estimate_wo_overhead ) as cfy_estimate_wo_overhead

    , sum( a.cfy_overhead ) as cfy_overhead

    , sum( a.cfy_estimate ) as cfy_estimate

    , sum( a.current_estimate_wo_overhead ) as current_estimate_wo_overhead

    , sum( a.fixed_costs ) as fixed_costs

    , sum( a.baseline_change ) as baseline_change

    , sum( a.program_change_1 ) as program_change_1

    , sum( a.program_change_2 ) as program_change_2

    , sum( a.program_change_3 ) as program_change_3

    , sum( a.total_direct_cost ) as total_direct_cost

    , sum( a.bfy_overhead ) as bfy_overhead

    , sum( a.bfy_estimate ) as bfy_estimate

 

    from (

 

    select a.service_provider_office as office

    , a.billing_type_title

    , a.passthrough_title

    , a.account_no

    , a.service_title as account_title

    , a.pres_bud as base_wo_overhead

    , a.adjustment

    , a.cfy_estimate as cfy_estimate_wo_overhead

    , 0 as cfy_overhead

    , a.cfy_estimate

    , a.cfy_estimate as current_estimate_wo_overhead

    , a.fixed_cost_calc as fixed_costs

    , a.baseline_change

    , a.program_change_1

    , a.program_change_2

    , a.program_change_3

    , a.bfy_estimate as total_direct_cost

    , 0 as bfy_overhead

    , a.bfy_estimate as bfy_estimate

 

    --    select a.*

    from bert.v_rfms_budget_history_grouped a

    left join (

      select b.fiscal_year

      , b.account_no

      , max( b.status_id ) as status_id

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.status_id <= nvl( p_new_status_id, 3 )

--      and b.status_id <= 1

 

      group by b.fiscal_year

      , b.account_no

    ) c

    on a.fiscal_year = c.fiscal_year

    and a.account_no = c.account_no

    and a.status_id = c.status_id

 

    where 1 = 1

    and a.billing_type != 'O'

    and a.count_in_boc_sum = '1'

    and a.passthrough = '0'

    and a.fiscal_year = p_fy

--    and a.fiscal_year = 2025

 

    and a.fund_center_overhead in (

      select distinct b.fund_center_overhead

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.rnk_change = 1

      and b.billing_type != 'O'

      and b.passthrough = '0'

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.fund_center like p_fund_center || '%'

--      and b.fund_center like 'DL900' || '%'

    )

 

    and ( 1 = 2

      or ( ( a.status_id_current = 3 ) and ( a.status_id = a.status_id_current ) )

      or ( ( a.status_id_current != 3 ) and ( a.status_id <= nvl( p_new_status_id, 3 ) ) and ( c.account_no is NOT NULL ) )

--      or ( 1 = 1 )

    )

 

    and ( 1 = 2

      or ( coalesce( p_is_budget_officer, '0' ) = '1' and a.is_budget_officer = 'Y' )

      or ( coalesce( p_is_budget_officer, '0' ) = '0' and a.is_budget_officer = 'N' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'Y' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'N' )

    )

 

    union all select a.service_provider_office as office

    , a.billing_type_title

    , a.passthrough_title

    , a.account_no

    , a.service_title as account_title

    , 0

    , 0

    , 0

    , a.cfy_estimate as cfy_overhead

    , a.cfy_estimate as cfy_estimate

    , 0

    , 0

    , 0

    , 0

    , 0

    , 0

    , 0

    , a.bfy_estimate as bfy_overhead

    , a.bfy_estimate as bfy_estimate

 

    --    select a.*

    from bert.v_rfms_overhead_grouped a

    left join (

      select b.fiscal_year

      , b.account_no

      , max( b.status_id ) as status_id

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.status_id <= nvl( p_new_status_id, 3 )

--      and b.status_id <= 1

 

      group by b.fiscal_year

      , b.account_no

    ) c

    on a.fiscal_year = c.fiscal_year

    and a.account_no = c.account_no

    and a.status_id = c.status_id

 

    where 1 = 1

    and a.count_in_boc_sum = '1'

    and a.passthrough = '0'

    and a.fiscal_year = p_fy

--    and a.fiscal_year = 2025

 

    and a.fund_center_overhead in (

      select distinct b.fund_center_overhead

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.rnk_change = 1

      and b.passthrough = '0'

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.fund_center like p_fund_center || '%'

--      and b.fund_center like 'DL900' || '%'

    )

 

    and ( 1 = 2

      or ( ( a.status_id_current = 3 ) and ( a.status_id = a.status_id_current ) )

      or ( ( a.status_id_current != 3 ) and ( a.status_id <= nvl( p_new_status_id, 3 ) ) and ( c.account_no is NOT NULL ) )

--      or ( 1 = 1 )

    )

 

    and ( 1 = 2

      or ( coalesce( p_is_budget_officer, '0' ) = '1' and a.is_budget_officer = 'Y' )

      or ( coalesce( p_is_budget_officer, '0' ) = '0' and a.is_budget_officer = 'N' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'Y' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'N' )

    )

 

    ) a

 

    group by grouping sets ( (), ( a.office

      , a.billing_type_title

      , a.passthrough_title

      , a.account_no

      , a.account_title   

    ) )

 

    order by a.account_no NULLS FIRST

    ;

 

    --  output Other Accounts

    OPEN resultset4 FOR

    select coalesce( a.office, 'TOTAL' ) as office

    , a.billing_type_title

    , a.passthrough_title

    , a.account_no

    , a.account_title

    , sum( base_wo_overhead ) as base_wo_overhead

    , sum( a.adjustment ) as adjustment

    , sum( a.cfy_estimate_wo_overhead ) as cfy_estimate_wo_overhead

    , sum( a.cfy_overhead ) as cfy_overhead

    , sum( a.cfy_estimate ) as cfy_estimate

    , sum( a.current_estimate_wo_overhead ) as current_estimate_wo_overhead

    , sum( a.fixed_costs ) as fixed_costs

    , sum( a.baseline_change ) as baseline_change

    , sum( a.program_change_1 ) as program_change_1

    , sum( a.program_change_2 ) as program_change_2

    , sum( a.program_change_3 ) as program_change_3

    , sum( a.total_direct_cost ) as total_direct_cost

    , sum( a.bfy_overhead ) as bfy_overhead

    , sum( a.bfy_estimate ) as bfy_estimate

 

    from (

 

    select a.service_provider_office as office

    , a.billing_type_title

    , a.passthrough_title

    , a.account_no

    , a.service_title as account_title

    , a.pres_bud as base_wo_overhead

    , a.adjustment

    , a.cfy_estimate as cfy_estimate_wo_overhead

    , 0 as cfy_overhead

    , a.cfy_estimate

    , a.cfy_estimate as current_estimate_wo_overhead

    , a.fixed_cost_calc as fixed_costs

    , a.baseline_change

    , a.program_change_1

    , a.program_change_2

    , a.program_change_3

    , a.bfy_estimate as total_direct_cost

    , 0 as bfy_overhead

    , a.bfy_estimate as bfy_estimate

 

    --    select a.*

    from bert.v_rfms_budget_history_grouped a

    left join (

      select b.fiscal_year

      , b.account_no

      , max( b.status_id ) as status_id

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.status_id <= nvl( p_new_status_id, 3 )

--      and b.status_id <= 1

 

      group by b.fiscal_year

      , b.account_no

    ) c

    on a.fiscal_year = c.fiscal_year

    and a.account_no = c.account_no

    and a.status_id = c.status_id

 

    where 1 = 1

    and a.billing_type != 'O'

    and a.count_in_boc_sum = '1'

    and a.passthrough = '1'

    and a.fiscal_year = p_fy

--    and a.fiscal_year = 2025

 

    and a.fund_center_overhead in (

      select distinct b.fund_center_overhead

 

      from bert.v_rfms_accounts_grouped b

      where 1 = 1

      and b.rnk_change = 1

      and b.fiscal_year = p_fy

--      and b.fiscal_year = 2025

      and b.fund_center like p_fund_center || '%'

--      and b.fund_center like 'DL900' || '%'

    )

 

    and ( 1 = 2

      or ( ( a.status_id_current = 3 ) and ( a.status_id = a.status_id_current ) )

      or ( ( a.status_id_current != 3 ) and ( a.status_id <= nvl( p_new_status_id, 3 ) ) and ( c.account_no is NOT NULL ) )

--      or ( 1 = 1 )

    )

 

    and ( 1 = 2

      or ( coalesce( p_is_budget_officer, '0' ) = '1' and a.is_budget_officer = 'Y' )

      or ( coalesce( p_is_budget_officer, '0' ) = '0' and a.is_budget_officer = 'N' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'Y' )

--      or ( ( 1 = 1 ) and a.is_budget_officer = 'N' )

    )

 

    ) a

 

    group by grouping sets ( (), ( a.office

      , a.billing_type_title

      , a.passthrough_title

      , a.account_no

      , a.account_title   

    ) )

 

    order by a.account_no NULLS FIRST

    ;

 

END;

 