## ����
1. ��ʼ����ǰ������һ�¡�L3 Exercise 2 - ��Python����AWS��ɫ��RedShift��Ⱥ�ȡ�
��һ����֮�󣬼ǵý���Ⱥɾ����
2. ÿ����������֮�󣬿��ܸ��������й���redshift��Host��ARN��

3. S3�ļ�������[����](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/?region=us-west-2&tab=overview);

4. log_data��song_dataֻ��һ���ļ��У�������ͬǰ׺������ʹ��copy����һ���Լ��أ�[copy�������jsonʾ���ĵ�](https://docs.aws.amazon.com/zh_cn/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json)�����п��ܻ��õ�һ��jsonpath����·��udacity-dend/log_json_path.json�£�

## ˼·
1. �ֱ���song��log��stage�����ڳн�copy��������������ô�sqlִ��ETL���̼��ɣ�

2. ά�����ʵ��Ľ������ɲο�P1;

3. 

## ��������
- copy data ���һ������45���ӣ�������10000��������
- �� stage �� user ��ʱ��Ŀǰ���õļ򵥵�distinct��ʵ��Ӧ����row_number�Ӳ�ѯ��ȡ���һ�ε�����

