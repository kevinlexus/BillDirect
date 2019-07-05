package com.dic.app.mm.impl;

import com.dic.app.mm.GenPenMng;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.SprPen;
import com.dic.bill.model.scott.Stavr;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.Optional;

/**
 * Сервис расчета пени
 *
 * @author Lev
 * @version 2.00
 */
@Slf4j
@Service
public class GenPenMngImpl implements GenPenMng {

    /**
     * Внутренний класс DTO расчета пени
     *
     * @author Lev
     */
    @Getter @Setter
    public class PenDTO {
        // кол-во дней просрочки
        int days = 0;
        // рассчитанная пеня
        BigDecimal penya;
        // % по которому рассчитана пеня (информационно)
        BigDecimal proc;
        // ставка рефинансирования, по которой расчитана пеня
        Stavr stavr;
    }

    /**
     * Рассчитать пеню
     *  @param deb - долг
     * @param mg    - период долга
     * @param kart  - лиц.счет
     * @return
     */
    @Override
    public Optional<PenDTO> calcPen(CalcStore calcStore, BigDecimal deb, Integer mg, Kart kart, Date curDt) {
        // дата начала начисления пени
        SprPen penDt = getPenDt(calcStore, mg, kart);
        // вернуть кол-во дней между датой расчета пени и датой начала пени по справочнику
        if (penDt == null) {
            if (mg.compareTo(calcStore.getPeriod()) > 0) {
                // период больше текущего, не должно быть пени
                return null;
            } else {
                // некритическая ошибка отсутствия записи в справочнике пени, просто не начислить пеню!
                log.error("ОШИБКА во время начисления пени по лиц.счету lsk={}, возможно не настроен справочник C_SPR_PEN!"
                                + "Попытка найти элемент: mg={}, kart.tp={}, kart.reu={}", kart.getLsk(),
                        mg, kart.getTp().getId(), kart.getUk().getReu());
                return null;
            }
        }
        int days = Utl.daysBetween(penDt.getDt(), curDt);
        if (days > 0) {
            // пеня возможна, если есть кол-во дней долга
            //log.info(" spr={}, cur={}, curDays={}", sprPenUsl.getTs(), curDt, curDays);
            Stavr stavr = calcStore.getLstStavr().stream()
                    .filter(t -> t.getTp().equals(kart.getTp())) // фильтр по типу лиц.счета
                    .filter(t -> days >= t.getDays1() && days <= t.getDays2()) // фильтр по кол-ву дней долга
                    .filter(t -> Utl.between(curDt, t.getDt1(), t.getDt2())) // фильтр по дате расчета в справочнике
                    .findFirst().orElse(null);
            PenDTO penDTO = new PenDTO();
            // расчет пени = долг * процент/100
            assert stavr != null;
            penDTO.proc = stavr.getProc();
            penDTO.penya = deb.multiply(penDTO.proc).divide(new BigDecimal(100), RoundingMode.HALF_UP);
            penDTO.days = days;
            penDTO.stavr = stavr;
            return Optional.of(penDTO);
        } else {
            // нет пени
            return Optional.empty();
        }
    }

    /**
     * Получить строку даты начала пени по типу лиц.счета
     *
     * @param calcStore - хранилище справочников
     * @param mg        - период задолженности
     * @param kart      - лиц.счет
     */
    private SprPen getPenDt(CalcStore calcStore, Integer mg, Kart kart) {
        return calcStore.getLstSprPen().stream()
                .filter(t -> t.getTp().equals(kart.getTp()) && t.getMg().equals(mg)) // фильтр по типу лиц.сч. и периоду
                .filter(t -> t.getReu().equals(kart.getUk().getReu()))
                .findFirst().orElse(null);
    }

}
