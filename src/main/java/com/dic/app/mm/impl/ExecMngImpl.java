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

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class ExecMngImpl implements ExecMng {

    @PersistenceContext
    private EntityManager em;
    private final SprGenItmDAO sprGenItmDao;
    private final ExecMngProc execMngProc;

    public ExecMngImpl(SprGenItmDAO sprGenItmDao, ExecMngProc execMngProc) {
        this.sprGenItmDao = sprGenItmDao;
        this.execMngProc = execMngProc;
    }

    /**
     * Обновить элемент меню
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateSprGenItem(List<SprGenItm> lst) {
        lst.forEach(t -> {
            SprGenItm sprGenItm = em.find(SprGenItm.class, t.getId());
            if (t.getSel() != null) {
                sprGenItm.setSel(t.getSel());
            }
        });
    }

    /**
     * Вызов процедуры в Oracle, при invalidate пакета - повторно
     *
     * @param var - вариант
     * @param id  - id внутреннего выбора
     * @param sel - дополнительный id
     */
    @Override
    public Integer execProc(Integer var, Long id, Integer sel) {
        try {
            return execMngProc.execSingleProc(var, id, sel);
        } catch (PersistenceException e) {
            if (e.getCause() != null && e.getCause() instanceof GenericJDBCException) {
                GenericJDBCException se = (GenericJDBCException) e.getCause();
                if (se.getErrorCode() == 4068) {
                    log.error("ОШИБКА! Пакет был изменен, ПОВТОРНЫЙ вызов процедуры execSingleProc " +
                            "с параметрами var={}, id={}, sel={}", var, id, sel);
                    // вызвать процедуру повторно
                    return execMngProc.execSingleProc(var, id, sel);
                } else {
                    log.error("ОШИБКА2! code={}", se.getErrorCode());
                    throw e;
                }

            } else {
                log.error("ОШИБКА! не обраб!={}", e.getCause().getMessage());
                throw e;
            }
        }

    }

    /**
     * Очистить ошибку формирования
     */
/*
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void clearError(SprGenItm menuGenItg) {
        //почистить % выполнения
        for (SprGenItm itm : sprGenItmDao.findAll()) {
            itm.setProc((double) 0);
            itm.setState(null);
            itm.setDt1(null);
            itm.setDt2(null);
        }
        menuGenItg.setState(null);
        menuGenItg.setErr(0);
        menuGenItg.setState(null);
        menuGenItg.setDt1(new Date());
    }
*/

    /**
     * Установить дату формирования
     */
/*
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void setGenDate() {
        execProc(16, null, null);
    }
*/

    /**
     * Закрыть или открыть базу для пользователей
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void stateBase(int state) {
        execProc(3, null, state);
    }

    /**
     * Установить процент выполнения в элементе меню
     *
     * @param spr  - элемент меню
     * @param proc - процент
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemPercent(SprGenItm spr, double proc) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setProc(proc);
    }

    /**
     * Установить строку состояния в элементе меню
     *
     * @param spr   - элемент меню
     * @param state - строка
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemState(SprGenItm spr, String state) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setState(state);
    }

    /**
     * Установить дату начала формирования в элементе меню
     *
     * @param spr  - элемент меню
     * @param dt1- дата начала формирования
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemDt1(SprGenItm spr, Date dt1) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setDt1(dt1);
    }

    /**
     * Установить окончания начала формирования в элементе меню
     *
     * @param spr  - элемент меню
     * @param dt2- дата начала формирования
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setMenuElemDt2(SprGenItm spr, Date dt2) {
        SprGenItm sprFound = em.find(SprGenItm.class, spr.getId());
        sprFound.setDt2(dt2);
    }

    /**
     * Почистить во всех элементах % выполения
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void clearPercent() {
        sprGenItmDao.findAll().forEach(t -> {
            t.setProc(0D);
            t.setDt1(null);
            t.setDt2(null);
            t.setState(null);
        });
    }

}