#导入必要的库
import requests
import pymysql
from pymysql.cursors import DictCursor
import re
import time
import urllib.parse
import datetime


# 请求头定制
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Mweibo-Pwa': '1',
    'Referer': 'https://m.weibo.cn/p/index?containerid=100808e94e8bd35fc8144f38fd1ebc1f81ab36_-_lbs&lcardid=frompoi&extparam=frompoi&luicode=10000011&lfid=100103type%3D1%26q%3D%E4%B8%8A%E6%B5%B7',
    'Sec-Ch-Ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': "Windows",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    # 此处需要修改
    'Cookie': '修改成个人的cookie'
}

# 城市字典
city_dict = {
    '北京': '100808692e92669c0ca340eff4fdcef32896ee_-_lbs',
    '上海': '100808e94e8bd35fc8144f38fd1ebc1f81ab36_-_lbs',
    '广州': '1008087e040aa9cb2ec494b0a4d52c147e682c_-_lbs',
    '深圳': '1008087a399889b9a4aed64afd0cf95941b975_-_lbs',
    '武汉': '100808ad9efa2f14d42f7d14ef876725909e27_-_lbs',
    '成都': '10080814bf5c897776f11648134a65c8365b77_-_lbs'
}

# 用户与POI爬取字段
poi_user_dict = {
    # poi相关数据
    'poi_id': '',
    'poi_category': '',
    'poi_lat': '',
    'poi_lng': '',
    'poi_name': '',
    'poi_address': '',
    # 用户相关数据
    'user_id': '',
    'user_name': '',
    'user_profile': '',
    'user_age': '',
    'user_xingzuo': '',
    'user_degree': '',
    'user_iphone': '',
    'user_sex': '',
    'user_company': '',
    'user_register_time': '',
    'user_credit_level': '',
    'user_followers': '',
    'user_friends': '',
    'user_share': '',
    'user_ip': '',
    'user_location': '',
    'user_auth': '',
    'user_svip_level': '',
    # 博客相关数据
    'blog_id': '',
    'blog_time_units': '',
    'blog_time_stamp': '',
    'blog_text': '',
    'blog_repost': '',
    'blog_attitude': '',
    'blog_comment': ''
}

def get_city_res(city, since_id, city_str):
    '''
    city_res 是关于指定城市下的博文内容，其中包括了blog_res、poi_res_1、poi_res_2、user_res_1、user_res_2获取的方法
    '''
    # 构造请求url
    city_url = f'https://m.weibo.cn/api/container/getIndex?containerid={city_dict[city]}&lcardid=frompoi&extparam=frompoi&luicode=10000011&lfid=100103type{city_str}'
    if since_id == 1:
        city_url = city_url
    else:
        city_url = city_url + f'&since_id={since_id}'
        
    city_res = requests.get(url=city_url, headers=headers, timeout=5).json()

    return city_res


def get_poi_res(pageinfo_url):
    '''
    poi_res_1 是关于poi 类别、名称
    poi_res_2 是关于poi id、经度、维度、地址
    '''
    poi_id = ''
    data = re.search(r'\?(.*)', pageinfo_url).group(1)
    pageinfo_url = f'https://m.weibo.cn/api/container/getIndex?{data}'
    poi_res_1 = requests.get(url=pageinfo_url, headers=headers, timeout=5).json()
    for e in poi_res_1['data']['cards'][0]['card_group']:
        # 先便利寻找poi_id获取的url
        if e['card_type'] != 101:
            continue
        else:
            poi_scheme = e['scheme']
            try:
                poi_id = re.search(r'poiid=([^&]+)', poi_scheme).group(1)
            except:
                poi_id = re.search(r'poiid=([^&]+)_pic', poi_scheme).group(1)

    poi_res_2 = requests.get(url=f'https://place.weibo.com/wandermap/pois?&poiid={poi_id}', headers=headers, timeout=5).json()


    return poi_res_1, poi_res_2

def get_user_res(uid):
    '''
    user_res_1、user_res_2 是关于用户详细信息
    '''
    user_url_1 = f'https://weibo.com/ajax/profile/info?uid={uid}'
    user_url_2 = f'https://weibo.com/ajax/profile/detail?uid={uid}'

    user_res_1 = requests.get(url=user_url_1, headers=headers, timeout=5).json()
    user_res_2 = requests.get(url=user_url_2, headers=headers, timeout=5).json()

    return user_res_1, user_res_2


def get_user_age(birthday):
    '''
    计算用户年龄
    '''
    birth = datetime.datetime.strptime(birthday, "%Y-%m-%d")
    today = datetime.datetime.today()
    return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))


def text_clean(text):
    '''
    初步的处理爬取下来的博文内容
    '''
    # 移除最后的地点名称标签（支持转义双引号）
    text = re.sub(r'<span class=["\']surl-text["\']>.*?</span></a>', '', text)
    # 移除HTML标签
    text = re.sub(r'<[^>]+>', '', text)
    # 移除特殊HTML实体
    text = re.sub(r'&[^;]+;', '', text)
    # 删除多余空格（连续空格变单个，首尾空格删除）
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def stop_time(timestamp_str):
    """
    判断是否回溯到设定时间
    """
    # 解析输入的时间戳字符串
    input_time = datetime.datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S %z %Y')

    # 定义目标日期（包含时区信息）
    # 此处用户可以自定义修改截止的回溯时间
    target_date = datetime.datetime(2022, 7, 10, tzinfo=datetime.timezone.utc)

    # 比较两个时间点
    return input_time > target_date

def get_information(blog_res, poi_res_1, poi_res_2, user_res_1, user_res_2, city):
    """
    根据blog_res, poi_res_1, poi_res_2, user_res_1, user_res_2存储poi_user数据的
    """
    # poi相关数据赋值
    try:
        poi_user_dict['poi_id'] = poi_res_2['poiid']
    except:
        poi_user_dict['poi_id'] = ''
    try:
        poi_user_dict['poi_lat'] = poi_res_2['lat']
    except:
        poi_user_dict['poi_lat'] = ''
    try:
        poi_user_dict['poi_lng'] = poi_res_2['lng']
    except:
        poi_user_dict['poi_lng'] = ''
    try:
        poi_user_dict['poi_address'] = poi_res_2['address']
    except:
        poi_user_dict['poi_address'] = ''
    try:
        poi_user_dict['poi_category'] = poi_res_1['data']['cards'][0]['card_group'][0]['group'][0]['item_desc'].split(' ')[0]
    except:
        poi_user_dict['poi_category'] = ''
    try:
        poi_user_dict['poi_name'] = poi_res_1['data']['cards'][0]['card_group'][0]['group'][0]['item_title']
    except:
        poi_user_dict['poi_name'] = ''
    # user相关数据赋值
    try:
        poi_user_dict['user_id'] = user_res_1['data']['user']['id']
    except:
        poi_user_dict['user_id'] = ''
    try:
        poi_user_dict['user_name'] = user_res_1['data']['user']['screen_name']
    except:
        poi_user_dict['user_name'] = ''
    try:
        poi_user_dict['user_profile'] = user_res_1['data']['user']['description']
    except:
        poi_user_dict['user_profile'] = ''
    try:
        poi_user_dict['user_age'] = user_res_2['data']['birthday'].split(' ')[0]
        poi_user_dict['user_age'] = get_user_age(poi_user_dict['user_age'])
    except:
        poi_user_dict['user_age'] = ''
    try:
        poi_user_dict['user_xingzuo'] = user_res_2['data']['birthday'].split(' ')[1]
    except:
        poi_user_dict['user_xingzuo'] = ''
    try:
        poi_user_dict['user_degree'] = user_res_2['data']['education']['school']
    except:
        poi_user_dict['user_degree'] = ''
    try:
        poi_user_dict['user_iphone'] = blog_res['mblog']['source']
    except:
        poi_user_dict['user_iphone'] = ''
    try:
        poi_user_dict['user_sex'] = user_res_1['data']['user']['gender']
        if user_res_1['data']['user']['gender'] == 'm':
            poi_user_dict['user_sex'] = '男'
        elif user_res_1['data']['user']['gender'] == 'f':
            poi_user_dict['user_sex'] = '女'
        else:
            poi_user_dict['user_sex'] = '未知'
    except:
        poi_user_dict['user_sex'] = '未知'
    try:
        poi_user_dict['user_company'] = user_res_2['data']['career']['company']
    except:
        poi_user_dict['user_company'] = ''
    try:
        poi_user_dict['user_register_time'] = user_res_2['data']['created_at']
    except:
        poi_user_dict['user_register_time'] = ''
    try:
        poi_user_dict['user_credit_level'] = user_res_2['data']['sunshine_credit']['level']
    except:
        poi_user_dict['user_credit_level'] = ''
    try:
        poi_user_dict['user_followers'] = user_res_1['data']['user']['followers_count']
    except:
        poi_user_dict['user_followers'] = ''
    try:
        poi_user_dict['user_friends'] = user_res_1['data']['user']['friends_count']
    except:
        poi_user_dict['user_friends'] = ''
    try:
        poi_user_dict['user_share'] = user_res_1['data']['user']['status_total_counter']['total_cnt']
    except:
        poi_user_dict['user_share'] = ''
    try:
        poi_user_dict['user_ip'] = user_res_2['data']['ip_location'][5:]
    except:
        poi_user_dict['user_ip'] = '未知'
    try:
        poi_user_dict['user_location'] = blog_res['mblog']['region_name'].split(' ')[1]
    except:
        poi_user_dict['user_location'] = ''
    try:
        poi_user_dict['user_auth'] = user_res_1['data']['user']['verified']
    except:
        poi_user_dict['user_auth'] = '未知'
    try:
        poi_user_dict['user_svip_level'] = user_res_1['data']['user']['svip']
    except:
        poi_user_dict['user_svip_level'] = '0'
    # blog相关数据赋值
    try:
        poi_user_dict['blog_id'] = blog_res['mblog']['mid']
    except:
        poi_user_dict['blog_id'] = ''
    poi_user_dict['blog_time_units'] = ''
    try:
        poi_user_dict['blog_time_stamp'] = blog_res['mblog']['created_at']
    except:
        poi_user_dict['blog_time_stamp'] = ''
    try:
        poi_user_dict['blog_repost'] = blog_res['mblog']['reposts_count']
    except:
        poi_user_dict['blog_repost'] = ''
    try:
        poi_user_dict['blog_attitude'] = blog_res['mblog']['attitudes_count']
    except:
        poi_user_dict['blog_attitude'] = ''
    try:
        poi_user_dict['blog_comment'] = blog_res['mblog']['comments_count']
    except:
        poi_user_dict['blog_comment'] = ''
    try:
        poi_user_dict['blog_text'] = text_clean(blog_res['mblog']['text'])
    except:
        poi_user_dict['blog_text'] = ''


    connect_mysql(poi_user_dict, city)

    return stop_time(poi_user_dict['blog_time_stamp'])


def connect_mysql(poi_user_dict, city):
    '''
    连接本地数据库，将数据存储在mysql中
    '''
    # 数据库连接配置
    config = {
        'host': 'localhost',
        'user': '修改成个人用户名',
        'password': '修改成个人数据库密码',
        'database': 'weibo-poi',
        'charset': 'utf8mb4',
        'cursorclass': DictCursor
    }

    try:
        # 建立数据库连接
        with pymysql.connect(**config) as connection:
            with connection.cursor() as cursor:
                # 构建SQL插入语句
                columns = ', '.join(poi_user_dict.keys())
                placeholders = ', '.join(['%s'] * len(poi_user_dict))
                sql = f"INSERT INTO `poi-user-{city}` ({columns}) VALUES ({placeholders})"
                # 执行插入
                cursor.execute(sql, tuple(poi_user_dict.values()))
            # 提交事务
            connection.commit()
            print(f"成功插入poi_id为{poi_user_dict['poi_id']}，时间为{datetime.datetime.strptime(poi_user_dict['blog_time_stamp'], '%a %b %d %H:%M:%S %z %Y')}的记录")
    except pymysql.Error as e:
        print(f"数据库错误: {e}")
    except Exception as e:
        print(f"其他错误: {e}")


if __name__ == '__main__':
    city = input('请输入你要爬取的城市（北京、上海、广州、深圳、武汉、成都）：')
    city_str = urllib.parse.quote(f'=1&q={city}')
    since_id, cnt = 1, 0
    flag = True
    while flag:
        city_res = get_city_res(city,since_id,city_str)
        for blog_res in (city_res['data']['cards'][1]['card_group'] if since_id == 1 else city_res['data']['cards'][0]['card_group']):
            if blog_res['card_type'] != 9:
                continue
            # 尝试获取poi_id
            try:
                pageinfo_url = blog_res['mblog']['page_info']['page_url']
                uid = blog_res['mblog']['user']['id']
                if 'show' in pageinfo_url:
                    print("该条数据，无法直接提取poi_id，跳过")
                    continue
            except KeyError:
                print('该条数据，无法提取page_info从而提取poi_id，跳过')
                continue
            # 获取poi信息和user信息
            poi_res_1, poi_res_2 = get_poi_res(pageinfo_url)
            user_res_1, user_res_2 = get_user_res(uid)
            # 保存数据
            flag = get_information(blog_res, poi_res_1, poi_res_2, user_res_1, user_res_2, city)
            cnt += 1
            print(f'成功爬取1条数据，目前共爬取{cnt}条数据!')
        since_id += 1
        print("爬取1轮后，休眠5秒再次爬取")
        time.sleep(5)