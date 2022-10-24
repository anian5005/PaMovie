import unittest

from unittest.mock import patch

# Local application imports
from get_movie_data.rotten_tomotoes.update_rotten_tomatoes_id import (
    find_movie_on_tomato,
    rotten_tomatoes_movie_condition_filter)


class Resp(object): pass


class RottenTomatoesTestCrawler(unittest.TestCase):
    def setUp(self):
        resp = Resp()
        # read rotten tomatoes 'top gun' requests.text
        with open('rotten_tomatoes_search_response.txt', 'r') as file:
            data = file.read()

        resp.text = data
        resp.status_code = 200
        self.patcher = patch('requests.get', return_value=resp)
        self.patcher.start()

    def test_find_movie_on_rotten_tomatoes(self):
        correct_return_value = [
            {'year': '2022',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gun_maverick',
             'movie_name': 'Top Gun: Maverick',
             'cast': ['Tom Cruise', 'Miles Teller', 'Jennifer Connelly']
             },
            {'year': '2022',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gun_maverick_early_access_premiere_event_the_imax_2d_experience',
             'movie_name': 'Top Gun: Maverick - Early Access Premiere Event: The IMAX 2D Experience',
             'cast': ['Tom Cruise', 'Miles Teller', 'Jennifer Connelly']
             },
            {'year': '1996',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gun_over_moscow',
             'movie_name': 'Top Gun over Moscow',
             'cast': ['Jeff Ethell']
             },
            {'year': '1986',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gun',
             'movie_name': 'Top Gun',
             'cast': ['Tom Cruise', 'Kelly McGillis', 'Anthony Edwards']
             },
            {'year': '1955',
             'movie_url': 'https://www.rottentomatoes.com/m/10008578-top_gun',
             'movie_name': 'Top Gun',
             'cast': ['Sterling Hayden', 'William Bishop', 'Karin Booth']
             },
            {'year': '2022',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gunner_danger_zone',
             'movie_name': 'Top Gunner: Danger Zone',
             'cast': ['Michael Par矇', 'Michael Broderick', 'Anna Telfer']
             },
            {'year': '2020',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gunner',
             'movie_name': 'Top Gunner',
             'cast': ['Eric Roberts', 'Carol Anne Watts', 'Buck Burns']
             },
            {'year': '2012',
             'movie_url': 'https://www.rottentomatoes.com/m/soar_into_the_sun',
             'movie_name': 'Soar Into the Sun',
             'cast': ['Rain', 'Shin Se-kyung', 'Kim Sung-soo']},
            {'year': '2006',
             'movie_url': 'https://www.rottentomatoes.com/m/gunbuster_vs_diebuster_aim_for_the_top_the_gatta_movie',
             'movie_name': 'Gunbuster vs Diebuster: Aim for the Top-The GATTA Movie',
             'cast': ['Yukari Fukui', 'Noriko Hidaka', 'Maaya Sakamoto']}
        ]
        assert find_movie_on_tomato('top gun') == correct_return_value

    def test_rotten_tomatoes_movie_condition_filter(self):
        result_list = [
            {'year': '2022', 'movie_url': 'https://www.rottentomatoes.com/m/top_gun_maverick',
             'movie_name': 'Top Gun: Maverick',
             'cast': ['Tom Cruise', 'Miles Teller', 'Jennifer Connelly']},
            {'year': '2022',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gun_maverick_early_access_premiere_event_the_imax_2d_experience',
             'movie_name': 'Top Gun: Maverick - Early Access Premiere Event: The IMAX 2D Experience',
             'cast': ['Tom Cruise', 'Miles Teller', 'Jennifer Connelly']
             },
            {'year': '1996', 'movie_url': 'https://www.rottentomatoes.com/m/top_gun_over_moscow',
             'movie_name': 'Top Gun over Moscow',
             'cast': ['Jeff Ethell']
             },
            {'year': '1986', 'movie_url': 'https://www.rottentomatoes.com/m/top_gun',
             'movie_name': 'Top Gun',
             'cast': ['Tom Cruise', 'Kelly McGillis', 'Anthony Edwards']
             },
            {'year': '1955', 'movie_url': 'https://www.rottentomatoes.com/m/10008578-top_gun',
             'movie_name': 'Top Gun',
             'cast': ['Sterling Hayden', 'William Bishop', 'Karin Booth']
             },
            {'year': '2022', 'movie_url': 'https://www.rottentomatoes.com/m/top_gunner_danger_zone',
             'movie_name': 'Top Gunner: Danger Zone',
             'cast': ['Michael Par矇', 'Michael Broderick', 'Anna Telfer']
             },
            {'year': '2020', 'movie_url': 'https://www.rottentomatoes.com/m/top_gunner', 'movie_name': 'Top Gunner',
             'cast': ['Eric Roberts', 'Carol Anne Watts', 'Buck Burns']
             },
            {'year': '2012', 'movie_url': 'https://www.rottentomatoes.com/m/soar_into_the_sun',
             'movie_name': 'Soar Into the Sun',
             'cast': ['Rain', 'Shin Se-kyung', 'Kim Sung-soo']
             },
            {'year': '2006',
             'movie_url': 'https://www.rottentomatoes.com/m/gunbuster_vs_diebuster_aim_for_the_top_the_gatta_movie',
             'movie_name': 'Gunbuster vs Diebuster: Aim for the Top-The GATTA Movie',
             'cast': ['Yukari Fukui', 'Noriko Hidaka', 'Maaya Sakamoto']
             }
        ]
        correct_return_value = [
            {'year': '2022', 'movie_url': 'https://www.rottentomatoes.com/m/top_gun_maverick',
             'movie_name': 'Top Gun: Maverick',
             'cast': ['Tom Cruise', 'Miles Teller', 'Jennifer Connelly']
             },
            {'year': '2020',
             'movie_url': 'https://www.rottentomatoes.com/m/top_gunner',
             'movie_name': 'Top Gunner',
             'cast': ['Eric Roberts', 'Carol Anne Watts', 'Buck Burns']
             }
        ]
        assert rotten_tomatoes_movie_condition_filter('top gun', 2022, result_list) == correct_return_value

    def tearDown(self):
        self.patcher.stop()
