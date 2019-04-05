from discord.ext import commands
#from pymongo import MongoClient
#from motor.motor_asyncio import AsyncIOMotorClient
#wow two deprecated db imports
import discord
import wumpus_config
import asyncio
import random
import psutil
import json
import os
import logging
import rocksdb
from collections import namedtuple

TOTAL_MEMORY = 1025130496 #in bytes

#TODO:
#MAYBE REGEN BORING PHRASES

#db key-value:
#0x00+USERID+GUILDID - USER JSON
# {"total":0,"bom":{"word",count}}
#0x01+USERID+GUILDID+WORD - WORD JSON
# {"total":0,"next":{"word",count}}, word may be None
#all IDs stored as 8-byte arrays big-endian

#op format:
#two bytes of count, utf-8 encoded word

#STOPPED ON: updating speak function for new db, do i even need total storage?

Operation = namedtuple('Operation',['word','count'])

def user_key(uid,gid):
    return b'\x00' + uid.to_bytes(8,'big') + gid.to_bytes(8,'big')

def word_key(uid,gid,w):
    return b'\x01' + uid.to_bytes(8,'big') + gid.to_bytes(8,'big') + w.encode('utf-8')

def decode_key(k):
    if k[0] == b'\x00':
        return {
            uid = int.from_bytes(k[1:9],'big')
            gid = int.from_bytes(k[9:17],'big')
        }
    elif k[0] == b'\x01':
        return {
            uid = int.from_bytes(k[1:9],'big')
            gid = int.from_bytes(k[9:17],'big')
            word = k[17:].decode('utf-8')
        }

def encode_op(op):
    # input: (word, count)
    #return op.count.to_bytes(2,'big') + b'\x00' if op.word is None else op.word.encode('utf-8') #null word byte
    return op.count.to_bytes(2,'big') + b'' if op.word is None else op.word.encode('utf-8') #no word byte

def decode_op(op):
    #return Operation(None if op[2:] == b'\x00' else op[2:].decode('utf-8'), int.from_bytes(op[0:2],'big')) #null word byte
    return Operation(None if len(op) == 2 else op[2:].decode('utf-8'), int.from_bytes(op[0:2],'big')) #no word byte

class WumpusMerge(rocksdb.interfaces.MergeOperator):
    #NECESSARY OPERATIONS TO DEFINE:
    #1: Add word to BOM
    #2: Add word to word obj
    #OPERANDS: (word or bom, count)
    #expressed in DB as 30-byte int big-endian + arbitrarily long utf8 word

    def full_merge(key, existing, op_list):
        #assumes that each thing is unique
        if existing is not None:
            l = json.loads(existing.decode('utf-8'))
        else:
            l = {}
        for op in op_list:
            op_d = decode_op(op)
            if "total" in l: 
                l["total"] += op.count
            else:
                l["total"] = op.count
            if op.word in l["words"]:
                l["words"][op.word] += op.count
            else:
                l["words"][op.word] = op.count
        return (True,json.dumps(l).encode('utf-8'))


    def partial_merge(key, left, right):
        if left[2:] == right[2:]:
            try:
                c = (int.from_bytes(left[0:2],'big') + int.from_bytes(right[0:2],'big')).to_bytes(30,'big')
                return (True,c+left[2:])
            except OverflowError:
                # we somehow overflowed the 2-byte count
                return (False,None)
        else:
            return (False,None)

class Wumpus(commands.Bot):
    #subclassing Bot so i can store my own properites
    #ripped from altarrel
    def __init__(self, **kwargs):
        super().__init__(
            command_prefix="w"
        )
        dlogger = logging.getLogger('discord')
        dlogger.setLevel(logging.WARN)
        handler = logging.FileHandler(filename='wumpus_dpy.log', encoding='utf-8', mode='a')
        handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(lineno)d: %(message)s'))
        dlogger.addHandler(handler)
        logger = logging.getLogger('wumpus')
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(filename='wumpus.log', encoding='utf-8', mode='a')
        handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(lineno)d: %(message)s'))
        logger.addHandler(handler)
        #self.db_client = AsyncIOMotorClient()
        #self.db = self.db_client.wumpus
        self.db = rocksdb.DB("wumpus.db", rocksdb.Options(create_if_missing=True,merge_operator=WumpusMerge))
        self.remove_command("help")
        self.add_command(build)
        #self.add_command(speak)
        logger.info("Initialized Wumpus.")

    #async def on_ready(self):
        # maybe do things
    #    pass

    #async def on_guild_join(self, guild):
        # maybe do things
    #    pass

    #not gonna bother axing deleted channels, shouldn't be too bad

async def run(token):
    wumpus = Wumpus()
    try:
        await wumpus.start(token)
    except KeyboardInterrupt:
        await ctx.bot.db.close()
        await wumpus.logout()



# commands
@commands.command()
@commands.is_owner()
async def build(ctx):
    #MOVE THIS TO ONGUILDJOIN WHEN DONE
    #NEW ROCKSDB KEY-VALUE FUNCTIONALITY INVOLVES TWO KINDS OF KEYS
    logger = logging.getLogger('wumpus')
    logger.info("Commanded to build on server {} named {}".format(ctx.guild.id, ctx.guild.name))
    #mem_db = {}
    guild_id = ctx.guild.id
    save_name = "build_saves/{}.json".format(guild_id)
    try:
        with open(save_name) as f:
            save = json.load(f)
            logger.info("Save loaded")
    except OSError:
        save = {}
        for channel in ctx.guild.text_channels:
            save[channel.id] = {
                "message": None,
                "finished": False
            }
        logger.info("New save object created")
        #with open(save_name, 'w') as f:
        #    save = 
    for channel in ctx.guild.text_channels:
        if channel.id in save:
            if save[channel.id]["finished"]:
                continue
                logger.info("Skipping channel {} named {} as it is marked finished in the save".format(channel.id, channel.name))
            elif save[channel.id]["message"]:
                start_at = channel.get_message(save[channel.id]["message"])
                logger.info("Restarting collection of channel {} named {} at message {} sent {} by {} with contents {}".format(
                    channel.id,
                    start_at.id,
                    start_at.created_at.isoformat(),
                    start_at.author+"#"+start_at.discrim,
                    start_at.clean_content
                    ))
            else:
                start_at = None
        else:
            save[channel.id] = {
                "message": None,
                "finished": False
            }
            start_at = None
            logger.info("Starting collection of channel {} named {}".format(channel.id, channel.name))
        if not channel.permissions_for(ctx.guild.get_member(ctx.bot.user.id)).read_messages:
            continue
        msgs_until_save = 50
        msg_count = 0
        async for msg in channel.history(limit=None,before=start_at):
            #logger.info("Working on message {} sent {}".format(msg.id, msg.created_at.isoformat()))
            user_id = msg.author.id
            l = msg.clean_content
            for i, v in wumpus_config.replacements:
                l = l.replace(i,v)
            l = l.split()
            if len(l) == 0:
                continue
            ctx.bot.db.merge(user_key(user_id,guild_id),encode_op((l[0],1))) #inc total and word count for first word in user object
            for index, word in enumerate(l):
                if index == len(l)-1:
                    nxt = None
                else:
                    nxt = l[index+1]
                ctx.bot.db.merge(word_key(user_id,guild_id,word),(nxt,1))
            save[channel.id]["message"] = msg.id
            msg_count += 1
            if msgs_until_save <= 0:
                logger.info("{} messages analyzed in channel {}, current message timestamp is {}".format(
                    msg_count,
                    channel.name,
                    msg.created_at.isoformat()))
                with open(save_name, 'w') as f:
                    json.dump(save, f)
                msgs_until_save = 50
        save[channel.id]["finished"] = True

def markov_word(l):
    """expects a list of {word,freq} in l"""
    #used to have a total parameter, redundant
    words = []
    freqs = []
    total = 0
    for o in l:
        words.append(o['word'])
        freqs.append(o['freq'])
        total += o['freq']
    n = random.random() * total
    c = 0
    for f in freqs:
        n -= f
        if n <= 0:
            break
        c += 1
    return words[c]

@commands.command()
async def speak(ctx, member : discord.Member):
    user_b = await ctx.bot.db.get(user_key(member.id,ctx.guild.id))
    if user_b is None:
        await ctx.send("No messages gauged.")
        return
    user = json.loads(user_b)
    this_bom_word = markov_word(user['words']) #DO I EVEN NEED TOTALS?
    msg = this_bom_word
    current_word = await ctx.bot.db.words.find_one({
        "user_id": member.id,
        "word": this_bom_word
        })
    while True:
        if current_word['eom_count'] > 0:
            current_word['next'].append({'word': None, 'freq': current_word['eom_count']})
        next_word = markov_word(current_word['next'],current_word['total'])
        if next_word is None:
            break
        msg += " " + next_word
        current_word = await ctx.bot.db.words.find_one({
        "user_id": member.id,
        "word": next_word
        })
    await ctx.send(msg)


loop = asyncio.get_event_loop()
loop.run_until_complete(run(wumpus_config.token))