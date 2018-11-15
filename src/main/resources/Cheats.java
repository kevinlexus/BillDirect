
// Отношения в Entity:
// тип лог.счетчика
@ManyToOne(fetch = FetchType.LAZY)
@JoinColumn(name = "FK_TP", referencedColumnName = "ID")
private Lst tp;

// связь счетчика, направленная к нему
@OneToMany(fetch = FetchType.LAZY)
@JoinColumn(name = "NOD_DST", referencedColumnName = "ID")
@BatchSize(size = 50)
@Fetch(FetchMode.SUBSELECT) // убрал subselect, так как внезапно начало тормозить
private List<MeterLogGraph> inside=new ArrayList<MeterLogGraph>(0);


// JpaRepostiory DAO - Native Query
public interface AchargeDAO extends JpaRepository<Acharge, Integer> {

    /**
     * Получить сгруппированные записи начислений (полного начисления, без учета льгот),
     * связанных с услугой из ГИС ЖКХ по лиц.счету и периоду
     * @param lsk - лицевой счет
     * @param mg - период
     * @param orgId - Id организации, по которой выбирается начисление (для обработки справочника №1 (доп.услуг) или №51 (коммун.услуг))
     * @return
     */
    // МОЖЕТ НЕ РАБОТАТЬ КАК НАДО, СМОТРИ ПРИМЕР НИЖЕ!
    @Query(value = "select t2.id as \"ulistId\", sum(t2.summa) as \"summa\", sum(t2.vol) as \"vol\", sum(price) as \"price\" from ( "
            + "select u.id, s.grp, sum(t.summa) as summa, sum(t.testOpl) as vol, min(t.testCena) as price "
            + "from scott.a_charge2 t "
            + "join exs.servgis s on t.fk_usl=s.fk_usl "
            + "join exs.u_list u on s.fk_list=u.id "
            + "join exs.u_listtp tp on u.fk_listtp=tp.id "
            + "where t.lsk = ?1 and ?2 between t.mgFrom and t.mgTo "
            + "and NVL(tp.fk_eolink, ?3) = ?3 "
            + "and t.type = 0 "
            + "group by u.id, s.grp) t2 "
            + "group by t2.id", nativeQuery = true) <--
    Native Query

    List<SumChrgRec> getChrgGrp(String lsk, Integer period, Integer orgId);
}

// Еще пример с NativeQuery (Важно наличие и кол-во полей в Projection!)
// Не обязательно писать \" в имени полей! Главное порядок расположения!
// Так как hibernate проверяет поля в алфавитном порядке ->> orgId, uslId, summa!
    @Query(value = "select n.org as orgId, t.summa_it as summa, t.usl_id as uslId from SCOTT.ARCH_CHARGES t "
            + "left join SCOTT.A_NABOR2 n on t.lsk = n.lsk and t.usl_id=n.usl " +
            "	and t.mg between n.mgFrom and n.mgTo " +
            "	where t.mg=:period" +
            "	and t.lsk=:lsk", nativeQuery = true)


// JpaRepostiory DAO - Запрос по параметру:
    @Query("select t from User t where t.cd = :cd")
    User getByCd(@Param("cd") String cd);

    // JpaRepostiory DAO - Получение результата в DTO:
    @Query(value = "select t.usl.id as uslId, t.org.id as orgId, t.summa as summa, t.penya as penya, "
            + "t.mg as mg, 1 as tp from DebPenUsl t where t.period=:period and t.kart.lsk=:lsk ")
    List<SumRec> getDebit(@Param("lsk") String lsk, @Param("period") Integer period);

    ЕСЛИ нужно
    чтобы поле
    заполнялось как null в DTO,тогда просто не нужно его указывать в списке алиасов"as",не надо делать null as"blabla"

// JpaRepostiory DAO - Удалить записи
@Modifying
@Transactional
@Query(value = "delete from DebPenUsl t where t.period=:period and "
        + " t.kart.lsk=:lsk) "
)
    void delByLskPeriod(@Param("lsk") String lsk,@Param("period") Integer period);

// JpaRepostiory DAO - Удалить записи с EXISTS

@Modifying
@Transactional
@Query(value = "delete from DebPenUsl t where t.period=:period and "
        + "exists (select k from Kart k where t.kart.lsk=k.lsk and k.lsk=:lsk) ")
	void delByLskPeriod(@Param("lsk") String lsk,@Param("period") Integer period);

// Конструктор Sparkle, вызов:

        DebPenUsl debPenUsl=DebPenUsl.builder()
        .withKart(kart)
        .withUsl(em.find(Usl.class,t.getUslOrg().getUslId()))
        ...

// Durian diffplug:
        res.getNsiItemInfo().stream().forEach(Errors.rethrow().wrap(t->{
        updNsiList(lst,t,grp);
        }));


// DTO для хранения записи, полученной из метода в JpaRepository
public interface SumDebitRec {

    String getUsl();

    Integer getOrg();

    BigDecimal getSumma();

    String getMg();

}

// Суррогатный ключ:

@IdClass(ApenyaId.class) // суррогатный первичный ключ
public class VchangeDet implements java.io.Serializable {

    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "LSK", referencedColumnName = "LSK")
    private Kart kart;

    // услуга
    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "USL", referencedColumnName = "USl", updatable = false, nullable = false)
    private Usl usl;

    // организация
    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "ORG", referencedColumnName = "ID", updatable = false, nullable = false)
    private Org org;
...
}

// Инициализация ArrayList в одну строку

    List<String> places = Arrays.asList("Buenos Aires", "Córdoba", "La Plata");

// Получить distinct значения выборки из одного поля

    List<String> lstUslId = lstFlow.stream().map(t -> t.getUslId()).distinct().collect(Collectors.toList());

    // Итерация итератором с удалением элемента (но без добавления!)
    Iterator<ResultSet> itr = lst.iterator();
	while(itr.hasNext()){
            ResultSet t=itr.next();
            lstRet.add(t);
            itr.remove();
            i++;
            if(i>cnt){
            break;
            }
            }
// Итерация и ДОБАВЛЕНИЕ элемента в тот же список ListIterator

            ListIterator<SumPenRec> itr=lst.listIterator();
        while(itr.hasNext()){
        SumPenRec t=itr.next();
        itr.add(sumPenRec);
        }

// Просуммировать все BigDecimal
        BigDecimal amnt=lst.stream()
        .reduce(BigDecimal.ZERO,BigDecimal::add);

// Получить MAX Integer
        Integer lastPeriod=lstDeb.stream().map(t->t.getMg())
        .reduce(Integer::max).orElse(null);


// Создать коллекцию другого объекта из stream
        lstDeb.addAll(lstFlow.stream()
        .map(t->new SumDebRec(t.getSumma().multiply(new BigDecimal("-1")),BigDecimal.ZERO,t.getMg()))
        .collect(Collectors.toList()));

// Сортировка без компаратора
// отсортировать по периоду
        List<SumDebRec> lstSorted=lst.stream().sorted((t1,t2)->
        t1.getMg().compareTo(t2.getMg())).collect(Collectors.toList());


// Сортировка компаратором

// отсортировать по лиц.счету
        Comparator<MeterDTO> byKartLsk=(e1,e2)->e1
        .getMeter().getMeterLog().getKart().compareTo(e2.getMeter().getMeterLog().getKart());
        return lst.stream().sorted(byKartLsk).collect(Collectors.toList());

// Транзакция

@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
public Future<Result> chrgAndSaveLsk(RequestConfig reqConfig,Integer kartId)throws ErrorWhileChrg,ExecutionException{

// Округлить до 2 знаков BigDecimal
// HALF_UP - для использования в деньгах!
        vl=vl.setScale(2,RoundingMode.HALF_UP);

// RuntimeException:

        try{
        // РАСЧЕТ задолжности и пени по услуге
        return debitThrMng.genDebitUsl(kart,t,calcStore,localStore).stream();
        }catch(ErrorWhileChrgPen e){
        e.printStackTrace();
        throw new RuntimeException("ОШИБКА в процессе начисления пени по лc.="+lsk);
        }

// Generics (Дженерики (Обобщения))
// интерфейс:

public interface ThreadMng<T> {

    void invokeThreads(CalcStore calcStore, int cntThreads, List<T> lstItem);

}

    // реализация:
    // получить следующий объект, для расчета в потоках
    private T getNextItem(List<T> lstItem) {
        Iterator<T> itr = lstItem.iterator();
        T item = null;
        if (itr.hasNext()) {
            item = itr.next();
            itr.remove();
        }

        return item;
    }

// Прочее //////////////////////////////////////////////////////////

    // Работа c кэшем
    @Cacheable(cacheNames = "MeterLogMngImpl.getKart", key = "{#rqn, #mLog.getId()}", unless = "#result == null")
    @Override
    public Kart getKart(int rqn, MLogs mLog) {
        return mDao.getKart(rqn, mLog);
    }

    // Удалить элементы кэша
    @CacheEvict(value = {"TarifMngImpl.getOrg", "KartMngImpl.getOrg"},
            allEntries = true)
    public Future<Result> chrgLsk(RequestConfig reqConfig, Kart kart,

// Вызов хранимой процедуры:
      @Override
      @Transactional(readOnly = false, propagation = Propagation.REQUIRED)
      public Integer execProc(Integer var, Integer id, Integer sel) {
        StoredProcedureQuery qr;
        Integer ret = null;
        Integer par = null;

        switch (var) {
            case 4:
            case 5:
            case 6:
            case 7:
            case 38:
                // проверка ошибок
                qr = em.createStoredProcedureQuery("scott.p_thread.smpl_chk");
                qr.registerStoredProcedureParameter(1, Integer.class, ParameterMode.IN);
                qr.registerStoredProcedureParameter(2, Integer.class, ParameterMode.OUT);
                // перекодировать в gen.smpl_chck код выполнения
                switch (var) {
                    case 4:
                        par = 1;
                        break;
                    case 5:
                        par = 2;
                        break;
                    case 6:
                        par = 3;
                        break;
                    case 7:
                        par = 4;
                        break;
                    case 38:
                        par = 5;
                        break;
                }
                qr.setParameter(1, par);
                qr.execute();
                ret = (Integer) qr.getOutputParameterValue(2);
                log.info("Проверка ошибок scott.p_thread.smpl_chk с параметром var_={}, дала результат err_={}", par, ret);
                break;


        }
    }