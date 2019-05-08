create or replace package rep_bills_compound is
  type ccur is ref cursor;
  tab tab_bill_detail;

procedure main(p_sel_obj in number, -- вариант выборки: 0 - по klsk, 1 - по адресу, 2 - по УК
               p_reu in kart.reu%type, -- код УК
               p_kul in kart.kul%type, -- код улицы
               p_nd in kart.nd%type,   -- № дома
               p_kw in kart.kw%type,   -- № квартиры
               p_lsk in kart.lsk%type, -- лиц.начальный
               p_lsk1 in kart.lsk%type,-- лиц.конечный
               p_firstNum in number, -- начальный номер счета (для печати по УК)
               p_lastNum in number,  -- конечный номер счета
               p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
               p_mg in params.period%type, -- период выборки
               p_sel_uk in varchar2, -- список УК
               p_rfcur out ccur -- исх.рефкурсор
  );

procedure contractors(p_klsk in number, -- klsk помещения
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_flt_tp in number, -- дополнительный фильтр
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur);

procedure getQr(p_klsk in number, -- klsk помещения
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_tp in number, -- 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем, 3 - вообще все
                 p_sel_flt_tp in number, -- дополнительный фильтр
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur
  );

procedure detail(p_klsk in number, -- klsk помещения
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_tp in number, -- 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем
                 p_sel_flt_tp in number, -- дополнительный фильтр
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur
  );
procedure funds_flow_by_klsk(
                 p_klsk in number, -- klsk помещения
                 p_sel_tp in number, -- 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем
                 p_sel_flt_tp in number, -- дополнительный фильтр
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur
  );

end rep_bills_compound;
/
create or replace package body rep_bills_compound is

procedure main(p_sel_obj in number, -- вариант выборки: 0 - по лиц.счету, 1 - по адресу, 2 - по УК
               p_reu in kart.reu%type, -- код УК
               p_kul in kart.kul%type, -- код улицы
               p_nd in kart.nd%type,   -- № дома
               p_kw in kart.kw%type,   -- № квартиры
               p_lsk in kart.lsk%type, -- лиц.начальный
               p_lsk1 in kart.lsk%type,-- лиц.конечный
               p_firstNum in number, -- начальный номер счета (для печати по УК)
               p_lastNum in number,  -- конечный номер счета
               p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
               p_mg in params.period%type, -- период выборки
               p_sel_uk in varchar2, -- список УК
               p_rfcur out ccur -- исх.рефкурсор
  ) is
begin

open p_rfcur for
select k.prn_num, k.for_bill, k.lsk, k.k_lsk_id, k.opl,
        utils.month_name(SUBSTR(p_mg, 5, 2)) || ' ' || SUBSTR(p_mg, 1, 4) || ' г.' as mg2,
        k.kpr,
        k.kpr_wr,
        k.kpr_wrp,
        k.kpr_ot,
        k.opl,
        t.name as st_name, -- статус
        decode(t.cd, 'MUN','Наниматель','Собственник') as pers_tp,
        s.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
        nvl(ltrim(k.kw, '0'), '0') as adr, -- адрес
        p_mg as mg, scott.utils.month_name(substr(p_mg, 5, 2))||' '||substr(p_mg,1,4)||'г.' as mg_str, -- период
        k.fio
    from scott.arch_kart k
      join v_lsk_tp tp on k.fk_tp=tp.id
      left join scott.spul s on k.kul = s.id
      left join scott.status t on k.status=t.id
   where k.mg = p_mg and 
                k.reu=p_reu and
                case when p_is_closed = 1 and nvl(k.for_bill,0)=1 then 1 -- либо в т.ч.закрытые, с долгом
                  when p_is_closed = 0 and k.psch not in (8,9) then 1 -- либо только открытые
                  else 0 end = 1 and
                decode(p_sel_obj, 0, p_lsk, k.lsk) >= k.lsk and  -- по лиц.счету
                decode(p_sel_obj, 0, p_lsk1, k.lsk) <= k.lsk and -- по лиц.счету
                decode(p_sel_obj, 1, nvl(p_kul,k.kul), k.kul) = k.kul and -- по адресу
                decode(p_sel_obj, 1, nvl(p_nd,k.nd), k.nd) = k.nd and
                decode(p_sel_obj, 1, nvl(p_kw,k.kw), k.kw) = k.kw and
                case when p_sel_obj = 2 and tp.cd='LSK_TP_MAIN' and k.prn_num between p_firstNum and p_lastNum then 1 -- по адресам УК, по диапазону pr_num
                 when p_sel_obj != 2 then 1
                else 0 end = 1
                and exists (
                  select * from arch_kart k2 where k2.mg = p_mg and k2.k_lsk_id=k.k_lsk_id 
                     and decode(p_sel_uk, '0', 1, instr(p_sel_uk, ''''||k2.reu||''';', 1)) > 0
                )
   order by k.prn_num;

/*  select k.prn_num, k.for_bill, k.lsk, k.k_lsk_id, k.opl,
        utils.month_name(SUBSTR(p_mg, 5, 2)) || ' ' || SUBSTR(p_mg, 1, 4) || ' г.' as mg2,
        k.kpr,
        k.kpr_wr,
        k.kpr_wrp,
        k.kpr_ot,
        k.opl,
        t.name as st_name, -- статус
        decode(t.cd, 'MUN','Наниматель','Собственник') as pers_tp,
        s.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
        nvl(ltrim(k.kw, '0'), '0') as adr, -- адрес
        p_mg as mg, scott.utils.month_name(substr(p_mg, 5, 2))||' '||substr(p_mg,1,4)||'г.' as mg_str, -- период
        k.fio
    from scott.arch_kart k
      join v_lsk_tp tp on k.fk_tp=tp.id
      left join scott.spul s on k.kul = s.id
      left join scott.status t on k.status=t.id
      join (select * from (-- фильтр по: открытый лиц.счет + NPP в v_lsk_tp выбрать приоритетный лиц.счет
              select k2.lsk,
               first_value(k2.lsk) over (partition by k2.k_lsk_id order by decode(k2.psch,8,1,9,1,0), tp2.npp) as lsk_main
               from ARCH_KART k2 join v_lsk_tp tp2 on k2.fk_tp=tp2.id and k2.mg = p_mg
               where
                decode(p_sel_uk, '0', 1, instr(p_sel_uk, ''''||k2.reu||''';', 1)) > 0 -- фильтр по УК
                and case when p_is_closed = 1 and nvl(k2.for_bill,0)=1 then 1 -- либо в т.ч.закрытые, с долгом
                  when p_is_closed = 0 and k2.psch not in (8,9) then 1 -- либо только открытые
                  else 0 end = 1 and
                decode(p_sel_obj, 0, p_lsk, k2.lsk) >= k2.lsk and  -- по лиц.счету
                decode(p_sel_obj, 0, p_lsk1, k2.lsk) <= k2.lsk and -- по лиц.счету
                decode(p_sel_obj, 1, nvl(p_kul,k2.kul), k2.kul) = k2.kul and -- по адресу
                decode(p_sel_obj, 1, nvl(p_nd,k2.nd), k2.nd) = k2.nd and
                decode(p_sel_obj, 1, nvl(p_kw,k2.kw), k2.kw) = k2.kw
                and exists (
                  select * from arch_kart k3 where k3.mg = p_mg and k3.k_lsk_id=k2.k_lsk_id and k3.psch not in (8,9) and
                  case when p_sel_obj = 2 and k3.reu=p_reu and k3.prn_num between p_firstNum and p_lastNum then 1 -- по адресам УК, по диапазону pr_num
                       when p_sel_obj != 2 then 1
                      else 0 end = 1
                )
              ) a where a.lsk=a.lsk_main) b on k.lsk=b.lsk
   where k.mg = p_mg 
   and decode(p_sel_uk, '0', 1, instr(p_sel_uk, ''''||k.reu||''';', 1)) > 0 -- фильтр по УК
   order by k.prn_num;
   */

end;

-- исполнители по ПД
procedure contractors(p_klsk in number, -- klsk помещения
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_flt_tp in number, -- дополнительный фильтр
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur
  ) is
begin

open p_rfcur for
  select a.*
  from (
  select k.lsk, k.k_lsk_id, k.opl,
         utils.month_name(SUBSTR(p_mg, 5, 2)) || ' ' || SUBSTR(p_mg, 1, 4) || ' г.' as period,
         last_day(to_date(p_mg||'01','YYYYMMDD')) as dt,
         t.name as st_name,
         decode(t.cd, 'MUN','Наниматель','Собственник') as pers_tp,
         s.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
         nvl(ltrim(k.kw, '0'), '0') as adr,
         h.postcode||','||o2.name||', ул.'||s.name || ', д. ' || nvl(ltrim(k.nd, '0'), '0') || ', кв.' ||
         nvl(ltrim(k.kw, '0'), '0') as adr2,
        case when stp.cd in ('LSK_TP_ADDIT') then o.r_sch_addit
                    else o3.raschet_schet end as raschet_schet,
        k.k_fam, k.k_im, k.k_ot, k.fio,
        o3.inn, o3.k_schet, o3.kpp, o3.bik, o3.bank, o.full_name, o3.phone, o3.adr as adr_org, o3.name as uk_name,
        o3.adr_www,
        k.psch, stp.cd as lsk_tp, stp.npp as lsk_tp_npp, k.prn_num, k.prn_new
    from scott.arch_kart k
      join scott.v_lsk_tp stp on k.fk_tp=stp.id
      join scott.spul s on k.kul = s.id
      join scott.status t on k.status=t.id
      join scott.t_org_tp tp on tp.cd='РКЦ'
      join scott.t_org o on tp.id=o.fk_orgtp
      join scott.t_org_tp tp2 on tp2.cd='Город'
      join scott.t_org o2 on tp2.id=o2.fk_orgtp
      join scott.t_org o3 on k.reu=o3.reu
      join scott.c_houses h on k.house_id=h.id
   where k.mg = p_mg and k.k_lsk_id =p_klsk
     and case when p_is_closed = 1 and nvl(k.for_bill,0)=1 then 1 -- либо в т.ч.закрытые, с долгом
              when p_is_closed = 0 and k.psch not in (8,9) then 1 -- либо только открытые
              else 0 end = 1
     and case when p_sel_flt_tp=0 then 1 -- все
          when p_sel_flt_tp=1 and stp.cd in ('LSK_TP_MAIN','LSK_TP_ADDIT') then 1 -- Основной+капрем.
          when p_sel_flt_tp=2 and stp.cd in ('LSK_TP_RSO') then 1 -- Только РСО
          else 0 end = 1
     and decode(p_sel_uk, '0', 1, instr(p_sel_uk, ''''||k.reu||''';', 1)) > 0 -- фильтр по УК
   order by k.prn_num
  ) a;

end;

-- получить QR код счета
procedure getQr(p_klsk in number, -- klsk помещения
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_tp in number, -- 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем, 3 - основные, 4 - все
                 p_sel_flt_tp in number, -- дополнительный фильтр: 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем, 3 - основные, 4 - все
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur
  ) is
begin

open p_rfcur for
  select 
   'ST00012'||'|Name='||a.full_name||'|PersonalAcc='||a.raschet_schet||
   '|BankName='||a.bank||'|BIC='||a.bik||'|CorrespAcc='||a.k_schet
   ||'|PayeeINN='||a.inn||'|persAcc='||a.lsk||'|' as QR, 
   case when lsk_tp='LSK_TP_ADDIT' then 'Фонд капремонта' else a.uk_name end as uk_name
  from (
  select k.lsk, k.k_lsk_id, k.opl,
         utils.month_name(SUBSTR(p_mg, 5, 2)) || ' ' || SUBSTR(p_mg, 1, 4) || ' г.' as period,
         last_day(to_date(p_mg||'01','YYYYMMDD')) as dt,
         t.name as st_name,
         decode(t.cd, 'MUN','Наниматель','Собственник') as pers_tp,
         s.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
         nvl(ltrim(k.kw, '0'), '0') as adr,
         o2.name||', ул.'||s.name || ', д. ' || nvl(ltrim(k.nd, '0'), '0') || ', кв.' ||
         nvl(ltrim(k.kw, '0'), '0') as adr2,
        case when stp.cd in ('LSK_TP_ADDIT') then o.r_sch_addit
                    else o3.raschet_schet end as raschet_schet,
        k.k_fam, k.k_im, k.k_ot, k.fio,
        o3.inn, o3.k_schet, o3.kpp, o3.bik, o3.bank, o.full_name, o3.phone, o3.adr as adr_org, o3.name as uk_name,
        o3.adr_www,
        k.psch, stp.cd as lsk_tp, stp.npp as lsk_tp_npp, k.prn_num, k.prn_new
    from scott.arch_kart k
      join scott.spul s on k.kul = s.id
      join scott.status t on k.status=t.id
      join scott.t_org_tp tp on tp.cd='РКЦ'
      join scott.t_org o on tp.id=o.fk_orgtp
      join scott.t_org_tp tp2 on tp2.cd='Город'
      join scott.t_org o2 on tp2.id=o2.fk_orgtp
      join scott.t_org o3 on k.reu=o3.reu
      join scott.v_lsk_tp stp on k.fk_tp=stp.id and -- фильтр по типу лиц.счетов
           case when p_sel_tp=0 and stp.cd not in 'LSK_TP_ADDIT' then 1 -- все, кроме капремонта
                when p_sel_tp=1 and stp.cd in 'LSK_TP_ADDIT' then 1 -- капрем.
                when p_sel_tp=3 and stp.cd in 'LSK_TP_MAIN' then 1 -- основные
                when p_sel_tp=4 then 1 -- вообще все
                else 0 end = 1
           and case when p_sel_flt_tp=0 then 1 -- все
                when p_sel_flt_tp=1 and stp.cd in ('LSK_TP_MAIN','LSK_TP_ADDIT') then 1 -- Основной+капрем.
                when p_sel_flt_tp=2 and stp.cd in ('LSK_TP_RSO') then 1 -- Только РСО
                else 0 end = 1
   where k.mg = p_mg and k.k_lsk_id =p_klsk
     and case when p_is_closed = 1 and nvl(k.for_bill,0)=1 then 1 -- либо в т.ч.закрытые, с долгом
              when p_is_closed = 0 and nvl(k.for_bill,0)=1 and k.psch not in (8,9) then 1 -- либо только открытые
              else 0 end = 1
     and decode(p_sel_uk, '0', 1, instr(p_sel_uk, ''''||k.reu||''';', 1)) > 0 -- фильтр по УК
   order by k.prn_num
  ) a;

end;

procedure detail(p_klsk in number, -- klsk помещения
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_tp in number, -- 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем, 3 - основные, 4 - все
                 p_sel_flt_tp in number, -- дополнительный фильтр: 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем, 3 - основные, 4 - все
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur
  ) is
begin

open p_rfcur for
  select a.*
  from (
  select k.lsk, k.k_lsk_id, k.opl,
         utils.month_name(SUBSTR(p_mg, 5, 2)) || ' ' || SUBSTR(p_mg, 1, 4) || ' г.' as period,
         last_day(to_date(p_mg||'01','YYYYMMDD')) as dt,
         t.name as st_name,
         decode(t.cd, 'MUN','Наниматель','Собственник') as pers_tp,
         s.name || ', ' || nvl(ltrim(k.nd, '0'), '0') || '-' ||
         nvl(ltrim(k.kw, '0'), '0') as adr,
         o2.name||', ул.'||s.name || ', д. ' || nvl(ltrim(k.nd, '0'), '0') || ', кв.' ||
         nvl(ltrim(k.kw, '0'), '0') as adr2,
        case when stp.cd in ('LSK_TP_ADDIT') then o.r_sch_addit
                    else o3.raschet_schet end as raschet_schet,
        s1.sal_in,
        s2.sal_out,
        k.k_fam, k.k_im, k.k_ot, k.fio,
        o3.inn, o3.k_schet, o3.kpp, o3.bik, o3.bank, o.full_name, o3.phone, o3.adr as adr_org, o3.name as uk_name,
        o3.adr_www,
        k.psch, stp.cd as lsk_tp, stp.npp as lsk_tp_npp, k.prn_num, k.prn_new,
        p1.penya_in, p2.penya_charge, p3.penya_corr,
        p4.penya_pay, p5.penya_out, p4.pay,
        p4.last_dtek, nvl(p6.charge,0) +
         case when p_is_closed = 0 and k.psch not in (8,9) or p_is_closed = 1 then nvl(p8.change,0)
            else 0 end as charge, null as qr -- для старых отчетов
    from scott.arch_kart k
      join scott.spul s on k.kul = s.id
      join scott.status t on k.status=t.id
      join scott.t_org_tp tp on tp.cd='РКЦ'
      join scott.t_org o on tp.id=o.fk_orgtp
      join scott.t_org_tp tp2 on tp2.cd='Город'
      join scott.t_org o2 on tp2.id=o2.fk_orgtp
      join scott.t_org o3 on k.reu=o3.reu
      join scott.v_lsk_tp stp on k.fk_tp=stp.id and -- фильтр по типу лиц.счетов
           case when p_sel_tp=0 and stp.cd not in 'LSK_TP_ADDIT' then 1 -- все, кроме капремонта
                when p_sel_tp=1 and stp.cd in 'LSK_TP_ADDIT' then 1 -- капрем.
                when p_sel_tp=3 and stp.cd in 'LSK_TP_MAIN' and k.psch not in (8,9) then 1 -- основные КРОМЕ ЗАКРЫТЫХ ТОЛЬКО в этом p_sel_tp=3!!! (Бред ред.29.12.18)
                when p_sel_tp=4 then 1 -- вообще все
                else 0 end = 1 and
           case when p_sel_flt_tp=0 then 1 -- все
                when p_sel_flt_tp=1 and stp.cd in ('LSK_TP_MAIN','LSK_TP_ADDIT') then 1 -- Основной+капрем.
                when p_sel_flt_tp=2 and stp.cd in ('LSK_TP_RSO') then 1 -- Только РСО
                else 0 end = 1
      left join (select l.lsk, sum(l.summa) as sal_in -- сальдо входящее
            from scott.saldo_usl l
           where l.mg=p_mg
           group by l.lsk) s1 on k.lsk=s1.lsk
      left join (select l.lsk, sum(l.summa) as sal_out -- сальдо исходящее
            from scott.saldo_usl l
           where l.mg=scott.utils.add_months_pr(p_mg, 1)
           group by l.lsk) s2 on k.lsk=s2.lsk
      left join (select l.lsk, sum(l.penya) as penya_in -- сальдо по пене входящее
            from scott.a_penya l
           where l.mg=scott.utils.add_months_pr(p_mg, -1)
           group by l.lsk) p1 on k.lsk=p1.lsk
      left join (select l.lsk, sum(l.penya) as penya_out -- сальдо по пене исходящее
            from scott.a_penya l
           where l.mg=p_mg
           group by l.lsk) p5 on k.lsk=p5.lsk
      left join (select l.lsk, sum(l.summa) as charge -- начисление текущее
            from scott.a_charge2 l
           where p_mg between l.mgFrom and l.mgTo and l.type=1
           group by l.lsk) p6 on k.lsk=p6.lsk
      left join (select l.lsk, sum(l.summa) as change -- перерасчеты
            from scott.a_change l
           where l.mg=p_mg
           group by l.lsk) p8 on k.lsk=p8.lsk
      left join (
           select l.lsk, sum(penya_charge) as penya_charge from (
             select t.lsk, t.mg1, round(sum(t.penya),2) as penya_charge -- начисление по пене текущее
              from scott.a_pen_cur t
             where t.mg=p_mg
             group by t.lsk, t.mg1) l
           group by l.lsk
           ) p2 on k.lsk=p2.lsk
      left join (select l.lsk, sum(l.penya) as penya_corr -- корректировки по пене текущие
            from scott.a_pen_corr l
           where l.mg=p_mg
           group by l.lsk) p3 on k.lsk=p3.lsk
      left join (select l.lsk, max(l.dtek) as last_dtek, -- дата платежа, последняя
           sum(l.summa) as pay, -- оплата текущая
           sum(l.penya) as penya_pay -- оплата по пене текущая
            from scott.a_kwtp_mg l
           where l.mg=p_mg
           group by l.lsk) p4 on k.lsk=p4.lsk
   where k.mg = p_mg and k.k_lsk_id =p_klsk
     and case when p_is_closed = 1 and nvl(k.for_bill,0)=1 then 1 -- либо в т.ч.закрытые, с долгом
              when p_is_closed = 0 and k.psch not in (8,9) then 1 -- либо только открытые
              else 0 end = 1
     and (nvl(p1.penya_in,0)<>0 or nvl(p2.penya_charge,0)<>0 or nvl(p3.penya_corr,0)<>0 or
        nvl(p4.penya_pay,0)<>0 or nvl(p5.penya_out,0)<>0 or nvl(p4.pay,0)<>0
        or (nvl(p6.charge,0) +
         case when p_is_closed = 0 and k.psch not in (8,9) or p_is_closed = 1 then nvl(p8.change,0)
            else 0 end) <> 0
        or p_is_closed = 1 and nvl(s2.sal_out,0)<>0
        or p_is_closed = 1 and nvl(p5.penya_out,0)<>0
      )
     and decode(p_sel_uk, '0', 1, instr(p_sel_uk, ''''||k.reu||''';', 1)) > 0 -- фильтр по УК
   order by stp.npp, decode(k.psch,8,999,9,999,0)
  ) a;

end;

-- движение средств по klsk
procedure funds_flow_by_klsk(
                 p_klsk in number, -- klsk помещения
                 p_sel_tp in number, -- 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем, 3 - основные, 4 - все
                 p_sel_flt_tp in number, -- дополнительный фильтр: 0 - тип лиц.счетов: Основные и РСО, 1 - кап.рем, 3 - основные, 4 - все
                 p_is_closed in number, -- выводить ли закрытый фонд, если есть долг? (0-нет, 1-да)
                 p_mg in params.period%type, -- период выборки
                 p_sel_uk in varchar2, -- список УК
                 p_rfcur out ccur
                 ) is
  cur1 SYS_REFCURSOR;
  type rec is record(
    is_amnt_sum      number,  -- суммировать в итог в fastreport
    usl         CHAR(3), -- код услуги
    npp           NUMBER, -- № п.п.
    name         VARCHAR2(100), -- наименование
    price        NUMBER, -- цена
    vol          NUMBER, -- объем
    charge       NUMBER, -- начисление
    change1      NUMBER, -- перерасчет
    change_proc1 NUMBER, -- % по перерасчету
    change2      NUMBER, -- перерасчет
    amnt         NUMBER, -- итого
    deb          NUMBER,  -- сальдо(задолженность)
    bill_col     number, -- в какой колонке выводить сумму (смотреть usl.bill.col)
    bill_col2     number, -- в какой колонке выводить сумму (смотреть usl.bill.col)
    kub          number,  -- объем ОДПУ
    pay          number, -- текущая оплата
    chargeOwn    number -- начисление на индивидуальное потребление (без значений usl.bill_col=1), в т.ч. перерасчеты
    );
  r rec;
  l_last number;
begin

  tab:= tab_bill_detail();
  for c in (select k.lsk, k.psch from arch_kart k join scott.v_lsk_tp stp on k.fk_tp=stp.id and -- фильтр по типу лиц.счетов
                     case when p_sel_tp=0 and stp.cd not in 'LSK_TP_ADDIT' then 1 -- все, кроме капремонта
                          when p_sel_tp=1 and stp.cd in 'LSK_TP_ADDIT' then 1 -- капрем.
                          when p_sel_tp=3 and stp.cd in 'LSK_TP_MAIN' then 1 -- основные
                          when p_sel_tp=4 then 1 -- вообще все
                          else 0 end = 1
                     and case when p_sel_flt_tp=0 then 1 -- все
                          when p_sel_flt_tp=1 and stp.cd in ('LSK_TP_MAIN','LSK_TP_ADDIT') then 1 -- Основной+капрем.
                          when p_sel_flt_tp=2 and stp.cd in ('LSK_TP_RSO') then 1 -- Только РСО
                          else 0 end = 1
                     where k.k_lsk_id=p_klsk and k.mg=p_mg
                          and decode(p_sel_uk, '0', 1, instr(p_sel_uk, ''''||k.reu||''';', 1)) > 0 -- фильтр по УК
                   ) loop

       rep_bills_ext.detail(p_lsk => c.lsk,
                     p_mg    => p_mg,
                     p_includeSaldo => 0,
                     p_rfcur => cur1);
       loop
         fetch cur1 into r;
         exit when cur1%notfound;
         tab.extend;
         l_last:=tab.last;
         tab(l_last):=rec_bill_detail(r.is_amnt_sum, r.usl,
           r.npp, r.name, r.price, r.vol,
           r.charge,
           case when p_is_closed = 0 and c.psch not in (8,9) or p_is_closed = 1 then r.change1 else 0 end,
           case when p_is_closed = 0 and c.psch not in (8,9) or p_is_closed = 1 then r.change_proc1 else 0 end,
           case when p_is_closed = 0 and c.psch not in (8,9) or p_is_closed = 1 then r.change2 else 0 end,
           r.amnt, r.deb, r.bill_col, r.bill_col2, r.kub, r.pay, r.chargeOwn);
       end loop;
  end loop;

  open p_rfcur for select * from table(tab) t
    where nvl(t.price,0)<>0 or nvl(t.charge,0)<>0
       or nvl(t.change1,0)<>0
       or nvl(t.change2,0)<>0
       --or t.amnt <>0
       or t.deb<>0 --or t.kub<>0
       ;
end;


end rep_bills_compound;
/
