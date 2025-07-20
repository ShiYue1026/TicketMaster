package com.damai.service.es;

import cn.hutool.core.collection.CollectionUtil;
import com.damai.core.SpringUtil;
import com.damai.dto.*;
import com.damai.enums.BusinessStatus;
import com.damai.page.PageUtil;
import com.damai.page.PageVo;
import com.damai.service.init.ProgramDocumentParamName;
import com.damai.service.tool.ProgramPageOrder;
import com.damai.util.StringUtil;
import com.damai.vo.ProgramHomeVo;
import com.damai.vo.ProgramListVo;
import com.github.pagehelper.PageInfo;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.filter.OrderedFormContentFilter;
import org.springframework.stereotype.Component;
import com.damai.util.BusinessEsHandle;

import javax.swing.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Slf4j
@Component
public class ProgramEs {

    @Autowired
    private BusinessEsHandle businessEsHandle;
    @Autowired
    private OrderedFormContentFilter formContentFilter;

    public List<ProgramHomeVo> selectHomeList(ProgramListDto programListDto) {
        List<ProgramHomeVo> programHomeVoList = new ArrayList<>();
        try{
            for (Long parentProgramCategoryId : programListDto.getParentProgramCategoryIds()) {
                List<EsDataQueryDto> programEsQueryDto = new ArrayList<>();
                if(Objects.nonNull(programListDto.getAreaId())){
                    // 地区id
                    EsDataQueryDto areaIdQueryDto = new EsDataQueryDto();
                    areaIdQueryDto.setParamName(ProgramDocumentParamName.AREA_ID);
                    areaIdQueryDto.setParamValue(programListDto.getAreaId());
                    programEsQueryDto.add(areaIdQueryDto);
                }else{
                    EsDataQueryDto primeQueryDto = new EsDataQueryDto();
                    primeQueryDto.setParamName(ProgramDocumentParamName.PRIME);
                    primeQueryDto.setParamValue(BusinessStatus.YES.getCode());
                    programEsQueryDto.add(primeQueryDto);
                }

                // 父节目类型id
                EsDataQueryDto parentProgramCategoryIdQueryDto = new EsDataQueryDto();
                parentProgramCategoryIdQueryDto.setParamName(ProgramDocumentParamName.PARENT_PROGRAM_CATEGORY_ID);
                parentProgramCategoryIdQueryDto.setParamValue(parentProgramCategoryId);
                programEsQueryDto.add(parentProgramCategoryIdQueryDto);

                // 只需要7条即可，使用分页查询
                PageInfo<ProgramListVo> pageInfo = businessEsHandle.queryPage(
                        SpringUtil.getPrefixDistinctionName() + "-" + ProgramDocumentParamName.INDEX_NAME,
                        ProgramDocumentParamName.INDEX_TYPE,
                        programEsQueryDto,
                        1,
                        7,
                        ProgramListVo.class
                );
                if(!pageInfo.getList().isEmpty()){
                    ProgramHomeVo programHomeVo = new ProgramHomeVo();
                    programHomeVo.setCategoryName(pageInfo.getList().get(0).getParentProgramCategoryName());
                    programHomeVo.setCategoryId(pageInfo.getList().get(0).getParentProgramCategoryId());
                    programHomeVo.setProgramListVoList(pageInfo.getList());
                    programHomeVoList.add(programHomeVo);
                }
            }
        } catch (Exception e){
            log.error("businessEsHandle.queryPage error",e);
        }

        return programHomeVoList;
    }

    public PageVo<ProgramListVo> selectPage(ProgramPageListDto programPageListDto) {
        PageVo<ProgramListVo> pageVo = new PageVo<>();
        try{
            List<EsDataQueryDto> programEsQueryDto = new ArrayList<>();
            if(Objects.nonNull(programPageListDto.getAreaId())){
                EsDataQueryDto areaIdQueryDto = new EsDataQueryDto();
                areaIdQueryDto.setParamName(ProgramDocumentParamName.AREA_ID);
                areaIdQueryDto.setParamValue(programPageListDto.getAreaId());
                programEsQueryDto.add(areaIdQueryDto);
            } else{
                EsDataQueryDto primeQueryDto = new EsDataQueryDto();
                primeQueryDto.setParamName(ProgramDocumentParamName.PRIME);
                primeQueryDto.setParamValue(BusinessStatus.YES.getCode());
                programEsQueryDto.add(primeQueryDto);
            }
            if(Objects.nonNull(programPageListDto.getParentProgramCategoryId())){
                EsDataQueryDto parentProgramCategoryIdQueryDto = new EsDataQueryDto();
                parentProgramCategoryIdQueryDto.setParamName(ProgramDocumentParamName.PARENT_PROGRAM_CATEGORY_ID);
                parentProgramCategoryIdQueryDto.setParamValue(programPageListDto.getParentProgramCategoryId());
                programEsQueryDto.add(parentProgramCategoryIdQueryDto);
            }
            if(Objects.nonNull(programPageListDto.getProgramCategoryId())){
                EsDataQueryDto programCategoryIdQueryDto = new EsDataQueryDto();
                programCategoryIdQueryDto.setParamName(ProgramDocumentParamName.PROGRAM_CATEGORY_ID);
                programCategoryIdQueryDto.setParamValue(programPageListDto.getProgramCategoryId());
                programEsQueryDto.add(programCategoryIdQueryDto);
            }
            if(Objects.nonNull(programPageListDto.getStartDateTime()) && Objects.nonNull(programPageListDto.getEndDateTime())){
                EsDataQueryDto showDayTimeQueryDto = new EsDataQueryDto();
                showDayTimeQueryDto.setParamName(ProgramDocumentParamName.SHOW_DAY_TIME);
                showDayTimeQueryDto.setStartTime(programPageListDto.getStartDateTime());
                showDayTimeQueryDto.setEndTime(programPageListDto.getEndDateTime());
                programEsQueryDto.add(showDayTimeQueryDto);
            }

            ProgramPageOrder programPageOrder = getProgramPageOrder(programPageListDto);

            PageInfo<ProgramListVo> pageInfo = businessEsHandle.queryPage(
                    SpringUtil.getPrefixDistinctionName() + "-" + ProgramDocumentParamName.INDEX_NAME,
                    ProgramDocumentParamName.INDEX_TYPE,
                    programEsQueryDto,
                    programPageOrder.sortParam,
                    programPageOrder.sortOrder,
                    programPageListDto.getPageNumber(),
                    programPageListDto.getPageSize(),
                    ProgramListVo.class
            );
            pageVo = PageUtil.convertPage(pageInfo, programListVo -> programListVo);
        } catch(Exception e){
            log.error("selectPage error",e);
        }
        return pageVo;
    }

    public ProgramPageOrder getProgramPageOrder(ProgramPageListDto programPageListDto){
        ProgramPageOrder programPageOrder = new ProgramPageOrder();
        switch (programPageListDto.getType()){
            // 推荐排序
            case 2:
                programPageOrder.sortParam = ProgramDocumentParamName.HIGH_HEAT;
                programPageOrder.sortOrder = SortOrder.DESC;
                break;
            // 最近开场
            case 3:
                programPageOrder.sortParam = ProgramDocumentParamName.SHOW_TIME;
                programPageOrder.sortOrder = SortOrder.ASC;
                break;
            // 最新上架
            case 4:
                programPageOrder.sortParam = ProgramDocumentParamName.ISSUE_TIME;
                programPageOrder.sortOrder = SortOrder.DESC;
                break;
            // 相关度排序
            default:
                programPageOrder.sortParam = null;
                programPageOrder.sortOrder = null;
        }
        return programPageOrder;
    }

    public PageVo<ProgramListVo> search(ProgramSearchDto programSearchDto) {
        PageVo<ProgramListVo> pageVo = new PageVo<>();
        try {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            if(Objects.nonNull(programSearchDto.getAreaId())){
                boolQuery.must(QueryBuilders.termQuery(ProgramDocumentParamName.AREA_ID, programSearchDto.getAreaId()));
            }
            if(Objects.nonNull(programSearchDto.getParentProgramCategoryId())){
                boolQuery.must(QueryBuilders.termQuery(ProgramDocumentParamName.PARENT_PROGRAM_CATEGORY_ID, programSearchDto.getParentProgramCategoryId()));
            }
            if(Objects.nonNull(programSearchDto.getProgramCategoryId())){
                boolQuery.must(QueryBuilders.termQuery(ProgramDocumentParamName.PROGRAM_CATEGORY_ID, programSearchDto.getProgramCategoryId()));
            }
            if(Objects.nonNull(programSearchDto.getStartDateTime()) && Objects.nonNull(programSearchDto.getEndDateTime())){
                boolQuery.must(QueryBuilders.rangeQuery(ProgramDocumentParamName.SHOW_DAY_TIME)
                        .from(programSearchDto.getStartDateTime()).to(programSearchDto.getEndDateTime()));
            }
            if(StringUtil.isNotEmpty(programSearchDto.getContent())){
                BoolQueryBuilder innerBoolQuery = QueryBuilders.boolQuery();
                innerBoolQuery.should(QueryBuilders.matchQuery(ProgramDocumentParamName.ACTOR, programSearchDto.getContent()));
                innerBoolQuery.should(QueryBuilders.matchQuery(ProgramDocumentParamName.TITLE, programSearchDto.getContent()));
                innerBoolQuery.minimumShouldMatch(1);
                boolQuery.must(innerBoolQuery);
            }

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            ProgramPageOrder programPageOrder = getProgramPageOrder(programSearchDto);
            if(Objects.nonNull(programPageOrder.sortParam) && Objects.nonNull(programPageOrder.sortOrder)){
                FieldSortBuilder sort = SortBuilders.fieldSort(programPageOrder.sortParam);
                sort.order(programPageOrder.sortOrder);
                searchSourceBuilder.sort(sort);
            }

            searchSourceBuilder.query(boolQuery);
            searchSourceBuilder.trackTotalHits(true);
            searchSourceBuilder.from((programSearchDto.getPageNumber() - 1) * programSearchDto.getPageSize());
            searchSourceBuilder.size(programSearchDto.getPageSize());
            searchSourceBuilder.highlighter(getHighlightBuilder(Arrays.asList(ProgramDocumentParamName.ACTOR, ProgramDocumentParamName.TITLE)));

            List<ProgramListVo> programListVoList = new ArrayList<>();
            PageInfo<ProgramListVo> pageInfo = new PageInfo<>(programListVoList);
            pageInfo.setPageNum(programSearchDto.getPageNumber());
            pageInfo.setPageSize(programSearchDto.getPageSize());

            businessEsHandle.executeQuery(
                    SpringUtil.getPrefixDistinctionName() + "-" + ProgramDocumentParamName.INDEX_NAME,
                    ProgramDocumentParamName.INDEX_TYPE,
                    programListVoList,pageInfo,
                    ProgramListVo.class,
                    searchSourceBuilder,
                    Arrays.asList(ProgramDocumentParamName.TITLE,ProgramDocumentParamName.ACTOR)
            );

            pageVo = PageUtil.convertPage(pageInfo,programListVo -> programListVo);
        } catch (Exception e) {
            log.error("search error", e);
        }
        return pageVo;
    }

    private HighlightBuilder getHighlightBuilder(List<String> fieldNameList) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        for (String fieldName : fieldNameList) {
            HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field(fieldName);
            highlightTitle.preTags("<em>");
            highlightTitle.postTags("</em>");
            highlightBuilder.field(highlightTitle);
        }
        return highlightBuilder;
    }

    public List<ProgramListVo> recommendList(ProgramRecommendListDto programRecommendListDto) {
        List<ProgramListVo> programListVoList = new ArrayList<>();
        try{
            boolean allQueryFlag = true;
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            if(Objects.nonNull(programRecommendListDto.getAreaId())) {
                allQueryFlag = false;
                QueryBuilder builds = QueryBuilders.termQuery(ProgramDocumentParamName.AREA_ID,
                        programRecommendListDto.getAreaId());
                boolQuery.must(builds);
            }
            if(Objects.nonNull(programRecommendListDto.getParentProgramCategoryId())) {
                allQueryFlag = false;
                QueryBuilder builds = QueryBuilders.termQuery(ProgramDocumentParamName.PARENT_PROGRAM_CATEGORY_ID,
                        programRecommendListDto.getParentProgramCategoryId());
                boolQuery.must(builds);
            }
            if(Objects.nonNull(programRecommendListDto.getProgramId())) {
                allQueryFlag = false;
                QueryBuilder builds = QueryBuilders.termQuery(ProgramDocumentParamName.ID,
                        programRecommendListDto.getProgramId());
                boolQuery.mustNot(builds);
            }
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(allQueryFlag ? matchAllQueryBuilder : boolQuery);
            searchSourceBuilder.trackTotalHits(true);
            searchSourceBuilder.from(1);
            searchSourceBuilder.size(10);

            Script script = new Script("Math.random()");
            ScriptSortBuilder scriptSortBuilder = new ScriptSortBuilder(script,  ScriptSortBuilder.ScriptSortType.NUMBER);
            scriptSortBuilder.order(SortOrder.ASC);

            searchSourceBuilder.sort(scriptSortBuilder);

            businessEsHandle.executeQuery(
                    SpringUtil.getPrefixDistinctionName() + "-" + ProgramDocumentParamName.INDEX_NAME,
                    ProgramDocumentParamName.INDEX_TYPE,
                    programListVoList,
                    null,
                    ProgramListVo.class,
                    searchSourceBuilder,
                    null
            );
        } catch (Exception e) {
            log.error("recommendList error",e);
        }
        return programListVoList;
    }

    public void deleteByProgramId(Long programId){
        try {
            List<EsDataQueryDto> esDataQueryDtoList = new ArrayList<>();
            EsDataQueryDto programIdDto = new EsDataQueryDto();
            programIdDto.setParamName(ProgramDocumentParamName.ID);
            programIdDto.setParamValue(programId);
            esDataQueryDtoList.add(programIdDto);

            List<ProgramListVo> programListVos =
                    businessEsHandle.query(
                            SpringUtil.getPrefixDistinctionName() + "-" + ProgramDocumentParamName.INDEX_NAME,
                            ProgramDocumentParamName.INDEX_TYPE,
                            esDataQueryDtoList,
                            ProgramListVo.class);
            if (CollectionUtil.isNotEmpty(programListVos)) {
                for (ProgramListVo programListVo : programListVos) {
                    businessEsHandle.deleteByDocumentId(
                            SpringUtil.getPrefixDistinctionName() + "-" + ProgramDocumentParamName.INDEX_NAME,
                            programListVo.getEsId());
                }
            }
        }catch (Exception e) {
            log.error("deleteByProgramId error",e);
        }
    }
}
