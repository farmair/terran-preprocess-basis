# AWS Lambda Node.js 18.x 기반 이미지 사용
FROM public.ecr.aws/lambda/nodejs:20 as build

# 환경 변수 설정
ENV AWS_LAMBDA_FUNCTION_TIMEOUT=900
ENV TZ=Asia/Seoul

# 작업 디렉토리 설정
WORKDIR ${LAMBDA_TASK_ROOT}

# 의존성 복사 및 설치
COPY package.json package-lock.json* ./
RUN npm install

# 코드 복사
COPY . .

# 타임존 실제 적용
RUN ln -sf /usr/share/zoneinfo/Asia/Seoul /etc/localtime && \
    echo "Asia/Seoul" > /etc/timezone



# 핸들러 파일 이름은 필요에 따라 수정 (예: index.handler → lambda.js handler 함수)
CMD [ "index.handler" ]