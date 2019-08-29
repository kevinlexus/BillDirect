package com.dic.app.mm.impl;

import com.dic.app.mm.ExecMng;
import com.dic.app.mm.ExecMngProc;
import com.dic.bill.dao.SprGenItmDAO;
import com.dic.bill.model.scott.SprGenItm;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.exception.GenericJDBCException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.*;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class ExecMngProcImpl implements ExecMngProc {

    @PersistenceContext
    private EntityManager em;

    /**
     * Вызов процедуры в Oracle
     *
     * @param var - вариант
     * @param id  - id внутреннего выбора
     * @param sel - дополнительный id
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Integer execSingleProc(Integer var, Long id, Integer sel) {
        StoredProcedureQuery qr;
        Integer ret = null;

        // установить текущую дату, до выполнения любой процедуры
        qr = em.createStoredProcedureQuery("scott.init.set_date_for_gen");
        qr.executeUpdate();


        switch (var) {
            // проверки ошибок (оставил несколько проверок здесь - после распределения пени и после архивов)
            case 13:
            case 37:
                qr = em.createStoredProcedureQuery("scott.gen.gen_check");
                qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.OUT);
                qr.registerStoredProcedureParameter(2, String.class, ParameterMode.OUT);
                qr.registerStoredProcedureParameter(3, Integer.class, ParameterMode.IN);
                // перекодировать в gen.gen_check код выполнения
                int par = 0;
                switch (var) {
                    case 13:
                        par = 6;
                        break;
                    case 37:
                        par = 9;
                        break;
                }
                qr.setParameter(3, par);
                qr.execute();
                ret = (Integer) qr.getOutputParameterValue(1);
                log.info("Проверка ошибок scott.gen.gen_check с параметром var_={}, дала результат err_={}", par, ret);
            case 16:
                // установить текущую дату, до формирования
                qr = em.createStoredProcedureQuery("scott.init.set_date_for_gen");
                qr.executeUpdate();
                break;
            case 17:
                // чистить инф, там где ВООБЩЕ нет счетчиков (нет записи в c_vvod)
                qr = em.createStoredProcedureQuery("scott.p_thread.gen_clear_vol");
                qr.executeUpdate();
                break;
            case 19:
                // сальдо по лиц счетам
                qr = em.createStoredProcedureQuery("scott.gen.gen_saldo");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 20:
                // движение
                qr = em.createStoredProcedureQuery("scott.c_cpenya.gen_charge_pay_full");
                qr.executeUpdate();
                break;
            case 21:
                // распределение пени по исх сальдо
                qr = em.createStoredProcedureQuery("scott.c_cpenya.gen_charge_pay_pen");

                qr.registerStoredProcedureParameter(1, Date.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(2, Long.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.setParameter(2, 0L);
                qr.executeUpdate();
                break;
            case 22:
                // сальдо по домам
                qr = em.createStoredProcedureQuery("scott.gen.gen_saldo_houses");
                qr.executeUpdate();
                break;
            case 23:
                // начисление по услугам (надо ли оно кому???)
                qr = em.createStoredProcedureQuery("scott.gen.gen_xito13");
                qr.executeUpdate();
                break;
            case 24:
                // оплата по операциям Ф.3.1.
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito5");
                qr.executeUpdate();
                break;
            case 25:
                // оплата по операциям Ф.3.1. для оборотки
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito5_");
                qr.executeUpdate();
                break;
            case 26:
                // по УК-организациям Ф.2.4.
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito10");
                qr.executeUpdate();
                break;
            case 27:
                // по пунктам начисления
                qr = em.createStoredProcedureQuery("scott.gen.gen_opl_xito3");
                qr.executeUpdate();
                break;
            case 28:
                // архив, счета
                qr = em.createStoredProcedureQuery("scott.gen.prepare_arch");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 29:
                // задолжники
                qr = em.createStoredProcedureQuery("scott.gen.gen_debits_lsk_month");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 31:
                // cписки - changes
                qr = em.createStoredProcedureQuery("scott.c_exp_list.changes_export");
                qr.executeUpdate();
                break;
            case 32:
                // cписки - charges
                qr = em.createStoredProcedureQuery("scott.c_exp_list.charges_export");
                qr.executeUpdate();
                break;
            case 33:
                // cтатистика
                qr = em.createStoredProcedureQuery("scott.gen_stat.gen_stat_usl");
                qr.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
                qr.setParameter(1, null);
                qr.executeUpdate();
                break;
            case 35:
                // вызов из WebCtrl
                qr = em.createStoredProcedureQuery("scott.p_thread.check_itms");
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(2, Integer.class, ParameterMode.IN);

                qr.setParameter(1, id);
                qr.setParameter(2, sel);
                qr.executeUpdate();
                break;
            case 36:
                // перераспределение авансовых платежей
                qr = em.createStoredProcedureQuery("scott.c_dist_pay.dist_pay_lsk_avnc_force");
                qr.executeUpdate();
                break;
            case 38:
                // обмен с ЛК
                // импорт данных
                qr = em.createStoredProcedureQuery("scott.ext_pkg.imp_vol_all");
                qr.executeUpdate();

                // экспорт данных
                qr = em.createStoredProcedureQuery("scott.ext_pkg.exp_base");
                qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(2, String.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(3, String.class, ParameterMode.IN);
                qr.setParameter(1, 1);
                qr.setParameter(2, null);
                qr.setParameter(3, null);
                qr.executeUpdate();
                break;
            case 100:
                // распределить ОДН во вводах, где нет ОДПУ
                qr = em.createStoredProcedureQuery("scott.p_vvod.gen_dist_wo_vvod_usl");
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.setParameter(1, id);
                qr.executeUpdate();
                break;
            case 101:
                // распределить ОДН во вводах, где есть ОДПУ
                qr = em.createStoredProcedureQuery("scott.p_thread.gen_dist_odpu");
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.setParameter(1, id);
                qr.executeUpdate();
                break;
            case 102:
                // начислить пеню по домам
                qr = em.createStoredProcedureQuery("scott.c_cpenya.gen_charge_pay_pen_house");
                // id дома
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                //qr.setParameter(1, new Date());
                qr.setParameter(1, id);
                qr.executeUpdate();
                break;
            case 103:
                // расчитать начисление по домам
                qr = em.createStoredProcedureQuery("scott.c_charges.gen_charges");
                // id дома
                qr.registerStoredProcedureParameter(1, Long.class, ParameterMode.IN);
                qr.setParameter(1, id);
                qr.executeUpdate();
                break;

            default:
                break;
        }

        return ret;
    }
}