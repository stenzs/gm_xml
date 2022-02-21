import config
import xmltodict
import psycopg2
import re
from PIL import Image
import requests
from io import BytesIO
from datetime import datetime
import time


def job():
    try:
        print("I'm working...")
        start_time = datetime.now()
        for user in config.users_test_zapchasti:
            index_arr = []
            city_arr = []
            category_arr = []
            posts = []
            posts_crm_ids = []
            url = user["url"]
            response = requests.get(url)
            data = xmltodict.parse(response.content)
            ADS = data["Ads"]
            AD = ADS["Ad"]
            for post in AD:
                try:
                    post_dict = {}
                    for rows_index in post:
                        if type(post[rows_index]) == str:
                            post_dict[rows_index.lower()] = post[rows_index]
                        else:
                            if rows_index.lower() != "images" and post[rows_index] is not None:
                                try:
                                    subs = []
                                    for col_i in post[rows_index]:
                                        if type(post[rows_index][col_i] == str):
                                            subs.append({col_i.lower(): post[rows_index][col_i]})
                                    post_dict[rows_index.lower()] = subs
                                except Exception:
                                    pass
                        if rows_index.lower() == "images" and post[rows_index] is not None:
                            try:
                                images = post[rows_index]
                                for image in images:
                                    if type(images[image]) == list:
                                        image_list = images[image]
                                        subs = []
                                        for img in image_list:
                                            try:
                                                for i in img:
                                                    if i.lower() == "@url":
                                                        PHOTO = img[i]
                                                        subs.append(PHOTO)
                                            except Exception:
                                                pass
                                        post_dict[rows_index.lower()] = subs
                                    else:
                                        img = images[image]
                                        try:
                                            subs = []
                                            for i in img:
                                                if i.lower() == "@url":
                                                    PHOTO = img[i]
                                                    subs.append(PHOTO)
                                            post_dict[rows_index.lower()] = subs
                                        except Exception:
                                            pass
                            except Exception:
                                pass

                        if rows_index == "City":
                            city_arr.append(post[rows_index])
                        if rows_index == "Category":
                            category_arr.append(post[rows_index])
                        index_arr.append(rows_index)
                    try:
                        post_dict['images']
                    except Exception:
                        post_dict['images'] = [config.default_photo_zapchasti]
                    posts.append(post_dict)
                except Exception as e:
                    print("  !Ошибка чтения объявления из xml файла: ", e)
            print("\n***   ***   ***\n")
            print("Всего объявлений в xml файле: ", len(AD))
            print("Города размещения: ", set(city_arr))
            print(set(index_arr))
            print(set(category_arr))
            posts = {"posts": posts}
            db_posts = []




            for post in posts["posts"]:
                try:
                    # Проверка региона и города для других компаний
                    manager_phone = re.sub("\D", "", post["contactphone"])
                    if len(manager_phone) < 11:
                        manager_phone = "+7" + str(manager_phone)
                    if len(manager_phone) == 11:
                        manager_phone = "+" + str(manager_phone)
                    db_post = {
                        "user_id": user["id"],
                        "crm_id": post["id"],
                        "title": post["title"],
                        "manager_name": post["managername"],
                        "coordinates": "[\"" + "55.11068954325282" + "\",\"" + "61.35709154934786" + "\"]",
                        "price": int(post["price"]),
                        "trade": False,
                        "bymessages": True,
                        "byphone": True,
                        "manager_phone": manager_phone,
                        "contact": manager_phone,
                        "description": post["description"],
                        "location": post["address"],
                        "city": "RU$RU-CHE$Челябинск",
                        "subcategory": None,
                        "alias": None,
                        "additional_fields": [],
                        "photo_url": post["images"]
                        }
                    if post["category"].lower() in ["запчасти и аксессуары"]:
                        db_post["subcategory"] = "spare_parts"
                        db_post["alias"] = "transport,parts_and_auto,spare_parts"
                        db_posts.append(db_post)
                    posts_crm_ids.append(post["id"])
                except Exception as e:
                    print("  !Ошибка преобразования объявления в формат kvik: ", e)
            print("Количество постов для загрузки", len(db_posts))

            try:
                con = psycopg2.connect(database=config.db_database, user=config.db_user, password=config.db_password, host=config.db_host, port=config.db_port)
                cur = con.cursor()
                cur.execute('SELECT array_to_json(array_agg(row_to_json(t)))from (SELECT "id", "crm_id" FROM "public"."posts" WHERE "posts"."crm_id" IS NOT NULL AND user_id = ' + str(user["id"]) + ") t")
                results = cur.fetchall()[0][0]
                con.close()
                exist_posts_crm_ids = []
                if results is None:
                    results = []
                if len(results) > 0:
                    for zz in results:
                        exist_posts_crm_ids.append(zz["crm_id"])
                posts_for_upload = [x for x in posts_crm_ids if x not in exist_posts_crm_ids]
                posts_for_delete = [x for x in exist_posts_crm_ids if x not in posts_crm_ids]
                posts_for_update = list(set(posts_crm_ids) & set(exist_posts_crm_ids))
                # print("Посты для загрузки: ", posts_for_upload)
                # print("Посты для удаления: ", posts_for_delete)
                # print("Посты для обновления: ", posts_for_update)
                if len(posts_for_delete) > 0:
                    try:
                        con = psycopg2.connect(database=config.db_database, user=config.db_user, password=config.db_password, host=config.db_host, port=config.db_port)
                        cur = con.cursor()
                        sql = 'DELETE from "public"."posts" WHERE "posts"."user_id" = ' + str(user["id"]) + ' AND crm_id IN %s'
                        cur.execute(sql, (tuple(posts_for_delete),))
                        con.commit()
                        con.close()
                        print("Устаревшие посты успешено удалены")
                    except Exception as e:
                        print("  !Ошибка при удалении постов из БД: ", e)
                for ix in db_posts:
                        if ix["crm_id"] in posts_for_upload:
                            try:
                                headers = {'x-access-token': user["token"]}
                                r = requests.post(str(config.server_url) + "setPosts", headers=headers, json=ix)
                                post_id = r.json()["id"]
                                files = []
                                for i in ix["photo_url"]:
                                    response = requests.get(i)
                                    img = Image.open(BytesIO(response.content))
                                    img = img.convert("RGB")
                                    buf = BytesIO()
                                    img.save(buf, 'jpeg')
                                    buf.seek(0)
                                    image_bytes = buf.read()
                                    files.append(('files[]', ("None.webp", image_bytes)))
                                headers = {'x-access-token': user["token"]}
                                r = requests.post(config.images_server_url + str(user["id"]) + "/" + str(post_id), headers=headers, files=files)
                                print("Объявление ", post_id, " успешно загружено ", r.status_code)
                            except Exception as e:
                                print("  !Ошибка при загрузке поста в БД: ", e)
                        if ix["crm_id"] in posts_for_update:
                            try:
                                kvik_post_id = [item for item in results if item["crm_id"] == ix["crm_id"]][0]['id']
                                ix["post_id"] = kvik_post_id
                                headers = {'x-access-token': user["token"]}
                                r = requests.post(str(config.server_url) + "updateFull", headers=headers, json=ix)
                                post_id = r.json()["id"]
                                files = []
                                for i in ix["photo_url"]:
                                    response = requests.get(i)
                                    img = Image.open(BytesIO(response.content))
                                    img = img.convert("RGB")
                                    buf = BytesIO()
                                    img.save(buf, 'jpeg')
                                    buf.seek(0)
                                    image_bytes = buf.read()
                                    files.append(('files[]', ("None.webp", image_bytes)))
                                headers = {'x-access-token': user["token"]}
                                r = requests.post(config.images_server_url + str(user["id"]) + "/" + str(post_id),
                                                  headers=headers, files=files)
                                print("Объявление ", post_id, " успешно обновленно ", r.status_code)
                            except Exception as e:
                                print("  !Ошибка при обновлении поста в БД: ", e)
            except Exception as e:
                print("  !Ошибка подключения к БД: ", e)




        print("Время выполнения скрипта: ", datetime.now() - start_time)
        print("success job\n")
    except Exception as e:
        print("  !Ошибка выполнения функции: ", e)


while True:
    print("start loop")
    job()
    hours = 12
    time.sleep(hours * 60 * 60)
