'''
    Baseado no código: 
    https://github.com/hoanhan101/blockchain-db/blob/master/src/blockchain_db.py
    Author: Hoanh An (hoanhan@bennington.edu)
	Date: 12/3/2017
'''

import hashlib
import json
from time import time, ctime
from pymongo import MongoClient

max_nonce = 2 ** 32
init_reward = 50
block_reward_rate = 1000
difficulty_bits_block_rate = 100
difficulty_block_rate = 100

class IoTBlockchainDB(object):
    def __init__(self, 
                 MongoIP:str='127.0.0.1', 
                 MongoPort:int=27017,
                ):
        """
        Inicializando Blockchain
        """
        # Definindo cliente do MongoDB
        self.client = MongoClient(f'mongodb://{MongoIP}:{MongoPort}')

        # Criando banco com nome 'blockchain'
        self.db = self.client.blockchain

        # Criando coleção de documentos 'block'
        self.blocks = self.db.blocks

        # Salva todas as transações na memória,
        # só grava no banco de dados quando o minerador minera com sucesso um novo bloco
        self.transactions = []

        # Redefine o tempo decorrido e o hash_power para 0
        self.elapsed_time = 0   # segundos
        self.hash_power = 0     # hashes por segundo

    def reset(self):
        """
        Apaga o banco de dados e comece tudo de novo criando o bloco genesis.
        """
        self.db.blocks.drop()
        self.generate_genesis_block()

    def generate_genesis_block(self):
        """
        Gere um bloco de gênese com nenhum hash anterior e 0 nonce.
        """
        self.generate_next_block(previous_hash=None, nonce=0)

    def generate_next_block(self, nonce, previous_hash=None):
        """
        Gere um novo bloco no BlockChain. 
        :param nonce: O nonce que é calculado pela Prova de Trabalho.
        :param previous_hash: Hash do bloco anterior
        :return: Novo bloco
        """
        # Define um bloco
        block = {
            "previous_block": self.get_length(),
            'height': self.get_length() + 1,
            'timestamp': ctime(time()),
            'transactions': self.transactions,
            "merkle_root": self.find_merkle_root(self.get_transaction_ids()),
            'number_of_transaction': len(self.transactions),
            'nonce': nonce,
            'previous_hash': previous_hash or self.hash_json_object(self.get_last_block()),
            'block_reward': self.calculate_block_reward(),
            'difficulty_bits': self.calculate_difficulty_bits(),
            'difficulty': self.calculate_difficulty(),
            'elapsed_time': self.elapsed_time,
            'hash_power': self.hash_power
        }

        # Redefine a lista atual de transações
        self.transactions = []

        # Inserindo no banco de dados
        self.blocks.insert_one(block)

        print('Bloco #{0} adicionado ao blockchain'.format(block['height']))

        return block

    def add_transaction(self, sender, recipient, amount):
        """
        Adicione uma nova transação ao bloco.
        :param sender: Endereço do remetente
        :param recipient: Endereço do destinatário
        :param amount: Quantidade de tokens
        """
        # Prepara as informações da transação
        transaction_info = {
            'sender': sender,
            'recipient': recipient,
            'amount': amount,
        }

        # Obtém o id da transação fazendo hash de seu conteúdo
        transaction_id = self.hash_json_object(transaction_info)

        # Anexar à lista de transações
        self.transactions.append({
            'transaction_id': transaction_id,
            'transaction_info': transaction_info
        })

    def find_merkle_root(self, transaction_ids):
        """
        Encontre uma raiz merle para uma determinada lista de transações.
        :param transaction_ids: Lista de transações
        :retorno: valor de hash
        """
        # Exceção: se não houver ids de transação, retorne Nenhum
        if len(transaction_ids) == 0:
            return None

        # Caso base: se houver apenas 1 transação, retorne o hash dessa transação
        if len(transaction_ids) == 1:
            return transaction_ids[0]

        # Caso contrário, crie uma nova lista de hash
        new_list = []

        # Passe pela lista de transaction_ids, junte os pares de itens e adicione-os à nova lista
        for i in range(0, len(transaction_ids) - 1, 2):
            new_list.append(self.hash_string_pair(transaction_ids[i], transaction_ids[i + 1]))

        # Se o comprimento do transaction_ids for ímpar, o que significa que resta apenas um,
        # hash a última transação consigo mesma.
        if len(transaction_ids) % 2 == 1:
            new_list.append(self.hash_string_pair(transaction_ids[-1], transaction_ids[-1]))

        # Recursivamente faça tudo de novo até sobrar um
        return self.find_merkle_root(new_list)

    def mine_for_next_block(self):
        """
        Encontre o nonce para o próximo bloco e adicione-o à cadeia.
        """
        # Suponha que o endereço do remetente e do destinatário seja fixo ao minerar um bloco
        reward = {
            'sender': '00000000000000000000x0',
            'recipient': '00000000000000000000x1',
            'amount': self.calculate_block_reward()
        }

        # Pegue o último bloco
        last_block = self.get_last_block()
        last_difficulty_bits = last_block['difficulty_bits']

        # Defina o temporizador para calcular o tempo que leva para minerar um bloco
        start_time = time()

        # Encontre nonce para o próximo bloco, dado o último bloco e o nível de dificuldade
        next_nonce = self.calculate_nonce(last_block, last_difficulty_bits)

        # Checkpoint quanto tempo demorou para encontrar um resultado
        end_time = time()
        self.elapsed_time = end_time - start_time

        # Estime os hashes por segundo
        if self.elapsed_time > 0:
            self.hash_power = float(int(next_nonce) / self.elapsed_time)

        # Minerador de recompensas
        self.add_transaction(reward['sender'], reward['recipient'], reward['amount'])

        # Adiciona esse bloco à cadeia
        self.generate_next_block(next_nonce)

    def calculate_nonce(self, last_block, number_of_bits):
        """
        Calcule o nonce usando o algoritmo Proof Of Work.
        Com base na implementação em http://chimera.labs.oreilly.com/books/1234000001802/ch08.html#_proof_of_work_algorithm
        :param last_block: Último bloco
        :param number_of_bits: Número de bits de dificuldade
        :return: Int se for bem sucedido, Nenhum se falhar
        """
        # Calcular a dificuldade alvo
        target = 2 ** (256 - number_of_bits)

        # Aumente constantemente o nonce em 1 e adivinhe o nonce certo
        for nonce in range(max_nonce):
            string = (str(last_block) + str(nonce)).encode()
            hash_result = hashlib.sha256(string).hexdigest()

            # Check if the hash result is below the target
            if int(hash_result, 16) < target:
                return nonce

        return None

    def hash_json_object(self, json_object):
        """
        Crie um hash SHA-256 de um objeto JSON.
        :param json_object: objeto JSON
        :return: String como valor de hash
        """
        # Certifique-se de que os dados estejam ordenados, caso contrário, teria hashes inconsistentes
        json_string = json.dumps(json_object, sort_keys=True).encode()
        hash_string = hashlib.sha256(json_string).hexdigest()
        return hash_string

    def hash_string_pair(self, string_1, string_2):
        """
        Retorna um valor de hash para um determinado par de strings.
        :param string_1: String
        :param string_2: String
        :return: String como valor de hash
        """
        # Concatenar 2 strings e codificá-las
        temp_string = (string_1 + string_2).encode()

        # Obtém o valor do hash
        hash_string = hashlib.sha256(temp_string).hexdigest()
        return hash_string

    def calculate_block_reward(self):
        """
        Calcule a recompensa do bloco para o próximo bloco minerado.
        Reduza a recompensa pela metade para cada n blocos, até que eventualmente reduza para 0.
        :return: Int
        """
        # Pegue o último bloco
        last_block = self.get_last_block()

        # Se ainda não temos nenhum bloco, significa que estamos criando o bloco gênese
        # Retorna o init_reward = 50
        if last_block == None:
            return init_reward

        # Caso contrário, obtenha a última recompensa do bloco e sua altura
        current_reward = last_block['block_reward']
        current_height = last_block['height']

        # Se a recompensa do bloco atual for maior que 1 e sua altura for divisível por n
        # então divida a recompensa do bloco pela metade.
        if current_reward > 1 and current_height % block_reward_rate == 0:
            current_reward /= 2
            return current_reward
        # Se ficar abaixo de 1, não será dada mais recompensa!
        elif current_reward < 1:
            return 0
        else:
            return current_reward

    def calculate_difficulty_bits(self):
        """
        Calcule os bits de dificuldade para o próximo bloco minerado.
        Para cada n blocos, aumente os bits de dificuldade em 1.
        :return: Int
        """
        # Pegue o último bloco
        last_block = self.get_last_block()

        # Se ainda não temos nenhum bloco, significa que estamos criando o bloco gênese
        # Defina os bits de dificuldade para 0
        if last_block == None:
            return 0

        # Caso contrário, calcule a base de dificuldade em bits de dificuldade
        current_difficulty_bits = last_block['difficulty_bits']
        current_height = last_block['height']

        # Se a altura atual for divisível por n,
        # aumenta a dificuldade exponencialmente por potência de 2
        if current_height % difficulty_bits_block_rate == 0:
            current_difficulty_bits += 1
            return current_difficulty_bits
        else:
            return current_difficulty_bits

    def calculate_difficulty(self):
        """
        Calculate the difficulty for the next mined block.
        For every n blocks, since the difficulty bits is increased by 1,
        the difficulty will increase exponentially by the number of 2.
        :return: Int
        """
        # Get the last block
        last_block = self.get_last_block()

        # If we don't have any block yet, it means that we are creating the genesis block
        # Set the difficulty to 1
        if last_block == None:
            return 1

        # Otherwise, calculate the difficulty base on difficulty bits
        current_difficulty_bits = last_block['difficulty_bits']
        current_difficulty = last_block['difficulty']
        current_height = last_block['height']

        # If the current height is divisible by n,
        # increase the difficulty exponentially by power of 2
        if current_height % difficulty_block_rate == 0:
            current_difficulty_bits += 1
            difficulty = 2 ** current_difficulty_bits
            return difficulty
        else:
            return current_difficulty

    def get_length(self):
        """
        Get the length of BlockChain.
        :return: Int
        """
        return self.blocks.count_documents({})

    def get_last_n_blocks(self, number):
        """
        Get last n given number of blocks.
        :param number: Number of blocks
        :return: Dictionary as a list of blocks
        """
        return self.blocks.find({}, {'_id': 0}).sort([('height', -1)]).limit(number)

    def get_top_blocks(self, state, number):
        """
        Get a number of top blocks for a given state.
        :return: List of blocks in dictionary format
        """
        if state == 'difficulty':
            return self.blocks.find({}, {'_id': 0}).sort([('difficulty', -1)]).limit(number)
        elif state == 'elapsed_time':
            return self.blocks.find({}, {'_id': 0}).sort([('elapsed_time', -1)]).limit(number)
        elif state == 'block_reward':
            return self.blocks.find({}, {'_id': 0}).sort([('block_reward', -1)]).limit(number)
        elif state == 'hash_power':
            return self.blocks.find({}, {'_id': 0}).sort([('hash_power', -1)]).limit(number)
        elif state == 'height':
            return self.blocks.find({}, {'_id': 0}).sort([('height', -1)]).limit(number)
        elif state == 'nonce':
            return self.blocks.find({}, {'_id': 0}).sort([('nonce', -1)]).limit(number)
        elif state == 'number_of_transaction':
            return self.blocks.find({}, {'_id': 0}).sort([('number_of_transaction', -1)]).limit(number)
        else:
            return []

    def get_last_block(self):
        """
        Get last block of the chain.
        :return: Dictionary
        """
        return self.blocks.find_one({'height': self.get_length()}, {'_id': 0})

    def get_genesis_block(self):
        """
        Get first block of the chain.
        :return: Dictionary
        """
        return self.blocks.find_one({'height': 1}, {'_id': 0})

    def get_block(self, height):
        """
        Get a block given height number.
        :return: Dictionary
        """
        return self.blocks.find_one({'height': height}, {'_id': 0})

    def get_all_blocks(self):
        """
        Get the full BlockChain.
        :return: List of blocks in dictionary
        """
        all_blocks = self.blocks.find({}, {'_id': 0})
        return all_blocks

    def get_transaction_ids(self):
        """
        Get a list of transaction ids.
        :return: List of transaction ids.
        """
        transaction_ids = []
        for transaction in self.transactions:
            transaction_id = transaction['transaction_id']
            transaction_ids.append(transaction_id)
        return transaction_ids
