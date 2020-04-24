#!/usr/bin/env python3

# Copyright 2020 Oskar Sharipov <oskar.sharipov[at]tuta.io>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Tuple, Set, Dict
import threading
import os
import logging

import lmdb
import queue
from ahocorapy.keywordtree import KeywordTree
import telegram
from telegram.ext import (Updater, MessageHandler, CommandHandler, Filters,
    CallbackContext)


def transaction(env: lmdb.Environment, write=False):

    def decorator(func):

        def wrapper(chat_id, *args, **kwargs):
            subdb = env.open_db(str(chat_id).encode())
            with env.begin(subdb, write=write) as txn:
                return func(txn, *args, **kwargs)

        return wrapper

    return decorator


# Init lmdb environment with as many dbs as many chats use the bot.
db_env = lmdb.Environment(
    path='db',
    map_async=True, max_dbs=80, meminit=False,
)

write_queue: "Queue[Tuple[str, str, str]]" = queue.Queue()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

@transaction(db_env, write=True)
def __write_nomi(txn: lmdb.Transaction, nomi: str, user_id: str) -> None:
    txn.put(nomi.encode(), user_id.encode())


def __writing_circle() -> None:
    while True:
        args = write_queue.get()
        if args is None:
            break

        __write_nomi(*args)


def start_writing_circle() -> threading.Thread:
    thread = threading.Thread(target=__writing_circle)
    thread.start()
    return thread


def put_nomi(chat_id: str, nomi: str, user_id: str) -> None:
    write_queue.put((chat_id, nomi, user_id))


@transaction(db_env)
def get_by_nomi(txn: lmdb.Transaction, nomi: str) -> str:
    return txn.get(nomi.encode()).decode()


@transaction(db_env, write=True)
def rm_by_nomi(txn: lmdb.Transaction, nomi: str) -> bool:
    return txn.delete(nomi.encode())


@transaction(db_env)
def list_nomis(txn: lmdb.Transaction) -> List[Tuple[str, str]]:
    result: List[Tuple[str, str]] = []
    for nomi, user_id in txn.cursor():
        result.append(
            (nomi.decode(), user_id.decode())
        )
    return result


class BotActions:
    def md_link(nomi: str, user_id: str) -> str:
        return f'[{nomi}](tg://user?id={user_id})'


    def all_nomis(update: telegram.Update, context: CallbackContext) -> None:
        nomis_to_user_ids = list_nomis(str(update.message.chat.id))
        message_text = ' '.join(
            [BotActions.md_link(n, ui) for n, ui in nomis_to_user_ids]
        ) or '???'
        update.message.reply_markdown(message_text)


    def b_list_nomis(update: telegram.Update, context: CallbackContext) -> None:
        user_id_to_nomis: Dict[str, List[str]] = dict()
        for nomi, user_id in list_nomis(str(update.message.chat.id)):
            if user_id not in user_id_to_nomis:
                user_id_to_nomis[user_id] = []
            user_id_to_nomis[user_id].append(nomi)

        message_lines = [
            '{ui} → {nomis}'.format(
                ui=ui,
                nomis=', '.join(user_id_to_nomis[ui])
            )
            for ui in user_id_to_nomis
        ]
        message_text = '\n'.join(message_lines) or '???'
        update.message.reply_markdown(message_text)



    def set_nomi(update: telegram.Update, context: CallbackContext) -> None:
        nomis = update.message.text.split()[1:]
        user_id = str(update.message.reply_to_message.from_user.id)
        chat_id = str(update.message.chat.id)
        for nomi in nomis:
            put_nomi(chat_id, nomi, user_id)
        message_text = f'Ok. {user_id}: +{len(nomis)} nomis.'
        update.message.reply_markdown(message_text)


    def unset_nomi(update: telegram.Update, context: CallbackContext) -> None:
        nomis = update.message.text.split()[1:]
        chat_id = str(update.message.chat.id)
        success = 0
        for nomi in nomis:
            success += int(rm_by_nomi(chat_id, nomi))
        message_text = f'Ok. -{len(nomis)} nomis.'
        update.message.reply_markdown(message_text)


    def look_for_nomis(update: telegram.Update, context: CallbackContext) -> None:
        kwtree = KeywordTree(case_insensitive=True)
        for nomi, _ in list_nomis(str(update.message.chat.id)):
            kwtree.add(nomi)
        kwtree.finalize()
        user_ids: Set[str] = set()
        chat_id = update.message.chat.id
        for nomi in kwtree.search_all(update.message.text):
            user_ids.add(get_by_nomi(chat_id, nomi[0]))
        if not user_ids:
            return

        message_text = ' '.join(
            [BotActions.md_link('ping', user_id) for user_id in user_ids]
        )
        update.message.reply_markdown(message_text)



def main() -> None:
    token = os.getenv('TOKEN')
    updater = Updater(token, use_context=True)
    handlers = (
        CommandHandler(
            'list',
            BotActions.b_list_nomis
        ),
        CommandHandler(
            'set',
            BotActions.set_nomi,
            pass_args=True,
            filters=(Filters.regex(r'^/set(\s+\S{3,30})+$') & Filters.reply)
        ),
        CommandHandler(
            'unset',
            BotActions.unset_nomi,
            pass_args=True,
            filters=Filters.regex(r'^/unset(\s+\S{3,30})+$')
        ),
        MessageHandler(
            Filters.regex(r'(^|\s)@all($|\s)'),
            BotActions.all_nomis
        ),
        MessageHandler(
            Filters.regex('.+'),
            BotActions.look_for_nomis
        )
    )

    for handler in handlers:
        updater.dispatcher.add_handler(handler)

    try:
        writing_thread = start_writing_circle()
        updater.start_polling()
        while True: pass
    except KeyboardInterrupt:
        logging.info('wait now')
        write_queue.put(None)
        updater.stop()
        writing_thread.join()
        logging.info('bye')


if __name__ == '__main__':
    main()

