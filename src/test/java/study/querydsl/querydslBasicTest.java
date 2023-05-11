package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.annotation.Testable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

@SpringBootTest
@Transactional
public class querydslBasicTest {

    @Autowired
    EntityManager em;

    @BeforeEach
    public void before() {
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");

        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        // member1 을 찾아라
        Member findMemberJPA = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMemberJPA.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        Member findMember = queryFactory.select(member).from(member).where(member.username.eq("member1")).fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");


    }

    @Test
    public void saerch() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    /**
     * 결과 조회
     */
    @Test
    public void resultFetch() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        Member fetchOne = queryFactory
                .selectFrom(member)
                .fetchOne();

        Member fetchFirst = queryFactory
                .selectFrom(member)
                .fetchFirst();

        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults();

    }

    /**
     * 정렬
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     */
    @Test
    public void sort() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();
    }

    /**
     * 페이징
     */
    @Test
    public void paging() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .limit(2)
//                .fetchResults();
                .fetch();

    }

    /**
     * 집합(Aggregation)
     */
    @Test
    public void aggregation() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
    }

    /**
     * 그룹바이
     * <p>
     * 팀의 이름과 각 팀의 평균 연령을 구하라
     */
    @Test
    public void groupbby() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);
    }

    /**
     * 조인
     */
    @Test
    public void join() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Member> result = queryFactory
                .selectFrom(member)
//                .join(member.team, team) //inner join
                .leftJoin(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        System.out.println("result = " + result);
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void join2() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);


        List<Member> result = queryFactory.
                selectFrom(member)
                .where(member.username.eq(team.name))
                .fetch();
    }

    /**
     * 조인
     * 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     */
    @Test
    public void join3() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory.
                select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();
    }

    /**
     * 연관관계 없는 엔티티 외부 조인
     * 회원의 이름이 팀이름과 같은 대상 외부 조인
     */
    @Test
    public void join_on_no_relation() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .where(member.username.eq(team.name))
                .fetch();
        System.out.println("result = " + result);
    }

    /**
     * 패치조인
     */
    @Test
    public void fetch_join() {
        em.flush();
        em.clear();
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        Member member1 = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();
    }

    /**
     * 서브쿼리
     */
    @Test
    public void sub_query() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        // 서브쿼리 member 의 alias 를 다르게 지정해준다.
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory.selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();
    }

    /**
     * Case 문
     */
    @Test
    public void case_query() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();
    }

    /**
     * 프로젝션 반환
     */
    @Test
    public void simpleProjection() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();
    }


    /**
     *  프로젝션 반환 값이 2개 이상인경우
     */
    @Test
    public void simpleProejction2() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> tuple = queryFactory.select(member.username, member.age)
                .from(member)
                .fetch();
    }

    /**
     * DTO 조회 일반 JPQL
     */
    @Test
    public void dtoJPQL() {
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();
    }

    /**
     * QueyDSL DTO 프로퍼티 접근 방법
     * getter, setter 를 통해 들어간다.
     */
    @Test
    public void dtoQueryDslByProperty() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        queryFactory
                .select(Projections.bean(
                        MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
    }

    /**
     * QueyDSL DTO 필드접근방법
     */
    @Test
    public void dtoQueryDslByField() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
    }

    /**
     * QueyDSL DTO 생성자 접근방법
     */
    @Test
    public void dtoQueryDslByConstructor() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        queryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
    }

    /**
     * QueyDSL 동적 쿼리 BooleanBuilder
     */
    @Test
    public void dynamicQuery_booleanBuilder() {
        String usernameParam = null;
        Integer ageParam = null;

        List<Member> result = searchMember1(usernameParam, ageParam);
        System.out.println("result.size() = " + result.size());
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        BooleanBuilder builder = new BooleanBuilder();

        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
//                .where(builder)
                // 다중 파라미터 사용
                .where(userNameEq(usernameCond), ageEq(ageCond))
                .fetch();
    }

    private Predicate userNameEq(String usernameCond) {
        //삼항 연산자
        return usernameCond != null ? member.username.eq(usernameCond) : null;
//        if (usernameCond == null) {
//            return null;
//        }
//        return member.username.eq(usernameCond);
    }

    private Predicate ageEq(Integer ageCond) {
        if (ageCond == null) {
            return null;
        }
        return member.age.eq(ageCond);
    }

    /**
     * 벌크연산
     */
    @Test
    public void bulkUpdate() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        // 영속성 컨테이너를 업데이트 해주기위해 꼭 작성해야한다.
        em.flush();
        em.clear();
    }

}
