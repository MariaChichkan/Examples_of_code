from abc import abstractmethod, ABC


class Hero:
    def __init__(self):
        self.positive_effects = []
        self.negative_effects = []
        self.stats = {
            "HP": 128,  # health points
            "MP": 42,  # magic points,
            "SP": 100,  # skill points
            "Strength": 15,  # сила
            "Perception": 4,  # восприятие
            "Endurance": 8,  # выносливость
            "Charisma": 2,  # харизма
            "Intelligence": 3,  # интеллект
            "Agility": 8,  # ловкость
            "Luck": 1  # удача
        }

    def get_positive_effects(self):
        return self.positive_effects.copy()

    def get_negative_effects(self):
        return self.negative_effects.copy()

    def get_stats(self):
        return self.stats.copy()


class AbstractEffect(Hero, ABC):
    def __init__(self, base):
        self.base = base

    @abstractmethod
    def get_stats(self):
        pass

    @abstractmethod
    def get_positive_effects(self):
        return self.base.get_positive_effects()

    @abstractmethod
    def get_negative_effects(self):
        return self.base.get_negative_effects()


class AbstractPositive(AbstractEffect):

    def get_negative_effects(self):
        return self.base.get_negative_effects()


class AbstractNegative(AbstractEffect):

    def get_positive_effects(self):
        return self.base.get_positive_effects()


class Berserk(AbstractPositive):

    def get_stats(self):
        new_stats = self.base.get_stats()
        for x in ["Strength", "Endurance", "Agility", "Luck"]:
            new_stats[x] += 7
        for x in ["Perception", "Charisma", "Intelligence"]:
            new_stats[x] -= 3
        new_stats["HP"] += 50
        return new_stats

    def get_positive_effects(self):
        return self.base.get_positive_effects() + ["Berserk"]



class Blessing(AbstractPositive):

    def get_stats(self):
        new_stats = self.base.get_stats()
        for x in ["Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck"]:
            new_stats[x] += 2
        return new_stats

    def get_positive_effects(self):
        return self.base.get_positive_effects() + ['Blessing']


class Weakness(AbstractNegative):
    def get_stats(self):
        new_stats = self.base.get_stats()
        for x in ["Strength",  "Endurance",  "Agility"]:
            new_stats[x] -= 4
        return new_stats

    def get_negative_effects(self):
        return self.base.get_negative_effects() + ['Weakness']


class EvilEye(AbstractNegative):
    def get_stats(self):
        new_stats = self.base.get_stats()
        new_stats["Luck"] -= 10
        return new_stats

    def get_negative_effects(self):
        return self.base.get_negative_effects() + ['EvilEye']


class Curse(AbstractNegative):
    def get_stats(self):
        new_stats = self.base.get_stats()
        for x in ["Strength", "Perception", "Endurance", "Charisma", "Intelligence", "Agility", "Luck"]:
            new_stats[x] -= 2
        return new_stats

    def get_negative_effects(self):
        return self.base.get_negative_effects() + ['Curse']



# hero = Hero()
# print(hero.get_stats())
# brs1 = Berserk(hero)
# print(brs1.get_stats())
# print(brs1.get_positive_effects())
# brs2 = Berserk(brs1)
# cur1 = Curse(brs2)
# print(cur1.get_stats())
# print(cur1.get_positive_effects())
# print(cur1.get_negative_effects())
# #Снимаем эффект Bersek
# cur1.base = brs1
# print(cur1.get_stats())
# print(cur1.get_positive_effects())