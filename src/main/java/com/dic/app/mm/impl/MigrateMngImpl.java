package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.dic.app.mm.MigrateMng;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.SumDebMgRec;
import com.dic.bill.dto.SumDebUslMgRec;
import com.dic.bill.dto.SumRecMg;
import com.dic.bill.dto.SumUslOrgRec;

import lombok.extern.slf4j.Slf4j;

/**
 * Сервис для миграции данных в другие структуры
 * @author Lev
 *
 */
@Slf4j
@Service
@Scope("prototype")
public class MigrateMngImpl implements MigrateMng {

	@PersistenceContext
    private EntityManager em;
	@Autowired
	private SaldoUslDAO saldoUslDao;

	/**
	 * Перенести данные из таблиц Директ, в систему учета долгов
	 * по услуге, организации, периоду
	 * @param lsk - лицевой счет
	 * @param period - как правило предыдущий период, относительно текущего
	 */
	@Override
	public void migrateDeb(String lsk, Integer period) {

		// свернуть задолженность, учитывая переплату
		List<SumDebMgRec> lstDeb = getRolledDeb(lsk, period);

		// получить начисление по услугам и орг., по всем периодам задолжности
		List<SumDebUslMgRec> lstChrg = getChrg(lsk, lstDeb);

		// получить исходящее сальдо предыдущего периода
		List<SumDebUslMgRec> lstSal = getSal(lsk, period);
	}

	/**
	 * Получить исходящее сальдо предыдущего периода
	 * @param lsk - лиц.счет
	 * @param period - период
	 * @return
	 */
	private List<SumDebUslMgRec> getSal(String lsk, Integer period) {
		List<SumDebUslMgRec> lst =
				new ArrayList<SumDebUslMgRec>();
		List<SumUslOrgRec> lst2 =
				saldoUslDao.getSaldoUslByLsk(lsk, period);
		lst2.forEach(d-> {
			lst.add(SumDebUslMgRec.builder()
					.withUslId(d.getUslId())
					.withOrgId(d.getOrgId())
					.withSumma(d.getSumma())
					.withSign(d.getSumma().compareTo(BigDecimal.ZERO))
					.build()
					);
		});
		return lst;
	}

	/**
	 * Получить начисление, по всем периодам задолжности
	 * учитывая вес по суммам
	 * @param lsk - лиц.счет
	 * @param lstDeb - коллекция задолженностей
	 * @return
	 */
	private List<SumDebUslMgRec> getChrg(String lsk, List<SumDebMgRec> lstDeb) {
		List<SumDebUslMgRec> lst
			= new ArrayList<SumDebUslMgRec>();
		//BigDecimal amnt = BigDecimal.ZERO;
		// загрузить начисление по всем периодам задолженности
		lstDeb.forEach(t-> {
			List<SumUslOrgRec> lst2 =
					saldoUslDao.getChargeNaborByLsk(lsk, t.getMg());
			// заполнить по каждому периоду задолженности - строки начисления
			lst2.forEach(d-> {
				lst.add(SumDebUslMgRec.builder()
									.withMg(t.getMg())
									.withUslId(d.getUslId())
									.withOrgId(d.getOrgId())
									.withSumma(d.getSumma())
									.build()
						);
			});
		});

		// итого
		BigDecimal amnt = lst.stream().map(t->t.getSumma()).reduce(BigDecimal.ZERO, BigDecimal::add);
		// установить коэфф сумм по отношению к итогу
		lst.forEach(t-> {
			BigDecimal proc = t.getSumma().divide(amnt);
			// округлить и если меньше 0, то принять как 0.01 руб.
			proc = proc.setScale(2, RoundingMode.HALF_UP);
			if (proc.compareTo(BigDecimal.ZERO) == 0) {
				t.setSumma(new BigDecimal("0.01"));
			} else {
				t.setSumma(proc);
			}
		});

		return lst;
	}

	/**
	 * Свернуть задолженность, учитывая переплату
	 * @param lsk - лиц.счет
	 * @param period - период
	 * @return
	 */
	private List<SumDebMgRec> getRolledDeb(String lsk, Integer period) {
		// получить отсортированный список задолженностей по периодам (по предыдущему периоду)
		List<SumRecMg> lst =
				saldoUslDao.getVchargePayByLsk(lsk, period);
		ListIterator<SumRecMg> itr = lst.listIterator();
		// переплата
		BigDecimal ovrPay = BigDecimal.ZERO;
		List<SumDebMgRec> lstDeb = new ArrayList<SumDebMgRec>();
		// свернуть задолженность
		while (itr.hasNext()) {
			SumRecMg t = itr.next();
			if (t.getSumma().compareTo(BigDecimal.ZERO) > 0) {
				// переплата
				// взять сумму текущего периода, добавить переплату
				BigDecimal summa = t.getSumma().add(ovrPay);
				if (summa.compareTo(BigDecimal.ZERO) <= 0) {
					// переплата или 0
					if (itr.hasNext()) {
						// перенести переплату в следующий период
						ovrPay = summa;
					} else {
						// последний период, записать сумму с учетом переплаты
						ovrPay = BigDecimal.ZERO;
						lstDeb.add(SumDebMgRec.builder()
						.withMg(t.getMg())
						.withSumma(summa)
						.withSign(-1)
						.build());
					}
				} else {
					// остался долг, записать его
					ovrPay = BigDecimal.ZERO;
					lstDeb.add(SumDebMgRec.builder()
					.withMg(t.getMg())
					.withSign(1)
					.withSumma(summa).build());
				}
			}
		}
		return lstDeb;
	}



}