package com.dic.app.mm.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Service;

import com.dic.app.mm.DebitThrMng;
import com.dic.app.mm.GenPen;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.dto.SumDebRec;
import com.dic.bill.dto.SumPenRec;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DebitThrMngImpl implements DebitThrMng {

	@PersistenceContext
    private EntityManager em;

	/**
	 * Расчет задолжности и пени по услуге
	 * @param kart - лицевой счет
	 * @param u - услуга и организация
	 * @param calcStore - хранилище параметров и справочников
	 * @return
	 * @throws ErrorWhileChrgPen
	 */
	@Override
	public List<SumPenRec> genDebitUsl(Kart kart, UslOrg u, CalcStore calcStore, CalcStoreLocal localStore) throws ErrorWhileChrgPen {
		// дата начала расчета
		Date dt1 = calcStore.getDt1();
		// дата окончания расчета
		Date dt2 = calcStore.getGenDt();
		//List<SumRec> lstFlow = calcStore.getLstFlow();
		List<SumDebRec> lstDeb = new ArrayList<SumDebRec>();
		// РАСЧЕТ по дням
		Calendar c = Calendar.getInstance();
		List<SumDebRec> lstPenAllDays = new ArrayList<SumDebRec>(30);
		for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
			Date curDt = c.getTime();
			// является ли текущий день последним расчетным?
			boolean isLastDay = curDt.equals(dt2);

			// ЗАГРУЗИТЬ из общей коллекции выбранные финансовые операции на ТЕКУЩУЮ дату расчета curDt
			// задолженность предыдущего периода, текущее начисление
			lstDeb = localStore.getLstDebFlow().stream()
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(t.getSumma(), t.getSumma(), null, null, t.getMg(), t.getTp()))
					.collect(Collectors.toList());
			lstDeb.addAll(localStore.getLstChrgFlow().stream()
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(t.getSumma(), t.getSumma(), null, null, t.getMg(), t.getTp()))
					.collect(Collectors.toList()));

			// перерасчеты, включая текущий день
			lstDeb.addAll(localStore.getLstChngFlow().stream()
					.filter(t-> t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(t.getSumma(), t.getSumma(), null, null, t.getMg(), t.getTp()))
					.collect(Collectors.toList()));
			if (isLastDay) {
				// АКТУАЛЬНО только для последнего дня расчета:
				// вх.сальдо по пене
				lstDeb.addAll(localStore.getLstDebFlow().stream()
						.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
						.map(t-> new SumDebRec(null, null, t.getPenya(), null, t.getMg(), t.getTp()))
						.collect(Collectors.toList()));
				// корректировки начисления пени
				lstDeb.addAll(localStore.getLstPenChrgCorrFlow().stream()
						.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
						.map(t-> new SumDebRec(null, null, null, t.getPenya(), t.getMg(), t.getTp()))
						.collect(Collectors.toList()));
				// оплата пени TODO!!!
/*				lstDeb.addAll(lstFlow.stream()
						.filter(t-> Utl.in(t.getTp(), 4))
						.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
						.map(t-> new SumDebRec(null, null, null, t.getPenya(), t.getMg(), t.getTp()))
						.collect(Collectors.toList()));*/
			}
			// вычесть оплату долга - для расчета долга, включая текущий день (Не включая для задолжности для расчета пени)
			lstDeb.addAll(localStore.getLstPayFlow().stream()
					.filter(t-> t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(
							t.getDt().getTime() < curDt.getTime() ?  // (Не включая текущий день, для задолжности для расчета пени)
									t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO ,
							t.getDt().getTime() <= curDt.getTime() ? // (включая текущий день, для обычной задолжности)
									t.getSumma().multiply(new BigDecimal("-1")) : BigDecimal.ZERO,
							null, null,
							t.getMg(), t.getTp()))
							.collect(Collectors.toList()));
			// вычесть корректировки оплаты - для расчета долга, включая текущий день (Не включая для задолжности для расчета пени)
			lstDeb.addAll(localStore.getLstPayCorrFlow().stream()
					.filter(t-> t.getDt().getTime() <= curDt.getTime())
					.filter(t-> t.getUslId().equals(u.getUslId()) && t.getOrgId().equals(u.getOrgId()))
					.map(t-> new SumDebRec(
							t.getSumma().multiply(new BigDecimal("-1")) ,
							t.getSumma().multiply(new BigDecimal("-1")),
							null, null,
							t.getMg(), t.getTp()))
							.collect(Collectors.toList()));
			// объект расчета пени
			GenPen genPen = new GenPen(kart, u, curDt, calcStore);
			// добавить и сгруппировать все финансовые операции по состоянию на текущий день
			lstDeb.forEach(t-> genPen.addRec(t));
			// свернуть долги (учесть переплаты предыдущих периодов),
			// рассчитать пеню на определенный день, добавить в общую коллекцию по всем дням
			lstPenAllDays.addAll(genPen.getRolledDebPen(isLastDay));

		}
		// сгруппировать пеню и вернуть
		return getGroupingPenDeb(u, lstPenAllDays);

	}


	/**
	 * Сгруппировать по периодам пеню, и долги на дату расчета
	 * @param uslOrg - услуга и организация
	 * @param lst - долги по всем дням
	 * @throws ErrorWhileChrgPen
	 */
	private List<SumPenRec> getGroupingPenDeb(UslOrg uslOrg, List<SumDebRec> lst) throws ErrorWhileChrgPen {
		// получить долги на последнюю дату
		List<SumPenRec> lstDebAmnt =  lst.stream()
				.filter(t-> t.getIsLastDay() == true)
				.map(t-> new SumPenRec(uslOrg, t.getSummaDeb(), t.getSummaRollDeb(), t.getPenyaIn(),
						t.getPenyaCorr(), t.getDays(), t.getMg()))
				.collect(Collectors.toList());

		// сгруппировать начисленную пеню по периодам
		for (SumDebRec t :lst) {
			addPen(uslOrg, lstDebAmnt, t.getMg(), t.getPenyaCur(), t.getDays());
		}
		lstDebAmnt.forEach(t-> {
			// округлить начисленную пеню до копеек, сохранить, добавить корректировки
			t.setPenyaCur(t.getPenyaCur().setScale(2, RoundingMode.HALF_UP).add(t.getPenyaCorr()));
			// установить исходящее сальдо
			t.setPenyaOut(t.getPenyaIn().add(t.getPenyaCur()));
		});

		return lstDebAmnt;
	}

	/**
	 * добавить пеню по периоду в долги по последней дате
	 * @param uslOrg - услуга и организация
	 * @param lstDebAmnt - коллекция долгов
	 * @param mg - период долга
	 * @param penya - начисленая пеня за день
	 * @param days - дней просрочки (если не будет найден период в долгах, использовать данный параметр)
	 * @throws ErrorWhileChrgPen
	 */
	private void addPen(UslOrg uslOrg, List<SumPenRec> lstDebAmnt, Integer mg, BigDecimal penya, Integer days) throws ErrorWhileChrgPen {
		// найти запись долга с данным периодом
		SumPenRec recDeb = lstDebAmnt.stream()
				.filter(t-> t.getMg().equals(mg)).findFirst().orElse(null);
		if (recDeb != null) {
			// запись найдена, сохранить значение пени
			recDeb.setPenyaCur(recDeb.getPenyaCur().add(penya));
		} else {
			// запись НЕ найдена, создать новую, сохранить значение пени
			// вообще, должна быть найдена запись, иначе, ошибка в коде!
			throw new ErrorWhileChrgPen("Не найдена запись долга в процессе сохранения значения пени!");
		}
	}

}