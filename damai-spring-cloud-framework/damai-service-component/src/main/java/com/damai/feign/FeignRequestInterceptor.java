package com.damai.feign;

import com.damai.util.StringUtil;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Objects;

import static com.damai.constant.Constant.*;

@Slf4j
@AllArgsConstructor
public class FeignRequestInterceptor implements RequestInterceptor {

    private final String serverGray;

    @Override
    public void apply(RequestTemplate template) {
        try {
            RequestAttributes ra = RequestContextHolder.getRequestAttributes();
            if (Objects.nonNull(ra)) {
                ServletRequestAttributes sra = (ServletRequestAttributes) ra;
                HttpServletRequest request = sra.getRequest();
                String traceId = request.getHeader(TRACE_ID);
                String code = request.getHeader(CODE);
                String gray = request.getHeader(GRAY_PARAMETER);
                if (StringUtil.isEmpty(gray)) {
                    gray = serverGray;
                }
                template.header(TRACE_ID,traceId);
                template.header(CODE,code);
                template.header(GRAY_PARAMETER,gray);
            }
        }catch (Exception e) {
            log.error("FeignRequestInterceptor apply error",e);
        }
    }
}
