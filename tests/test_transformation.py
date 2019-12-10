import unittest
import progutils
import functions
import errors
import numpy
import pandas


class TestTransformation(unittest.TestCase):

    def setUp(self):
        self.trans_obj = progutils.Transformation()

    def new_trans_obj(self):
        return self.trans_obj.copy()

    def test_procedure_property(self):
        # Test if property procedures exists
        try:
            self.trans_obj.procedure
        except NameError:
            self.fail("Attribute `procedure` does not exist")

    def test_procedure_correct_type(self):
        self.assertIsInstance(self.trans_obj.procedure, list)

    def test_procedure_setter(self):
        tmp = self.new_trans_obj()
        tmp.procedure = [functions.t_log1p()]

        with self.assertRaises(TypeError):
            # Expecting a list, this should fail
            tmp.procedure = "should be sequence"

        with self.assertRaises(TypeError):
            # Expecting a list of functions (callable), this should fail
            tmp.procedure = ['should be function']

        try:
            tmp.procedure = [functions.t_log(), functions.t_sqrt()]
        except TypeError:
            self.fail("adding list of functions failed")

    def test_plan_property(self):
        tmp = self.new_trans_obj()
        self.assertIsNone(tmp.plan)

        tmp.add(functions.t_log())

        self.assertIsInstance(tmp.plan, str)
        self.assertTrue(tmp.plan.find('ln') != -1)

        tmp.add(functions.t_reverse())
        self.assertIsInstance(tmp.plan, str)
        self.assertTrue(tmp.plan.find('ln') != -1 and
                        tmp.plan.find('reverse') != -1 and
                        tmp.plan.find(' -> ') != -1)

    def test_copy_method(self):
        self.assertIsInstance(self.trans_obj.copy(), progutils.Transformation)

    def test_empty_property(self):
        self.assertTrue(self.trans_obj.empty)

    def test_add_function(self):
        tmp = self.new_trans_obj()

        with self.assertRaises(TypeError):
            tmp.add('hello')

        tmp.add(functions.t_log())

        self.assertEqual(tmp.size, 1)
        self.assertTrue(callable(tmp.get(0)))

        # Check if values come out the same (the functions)
        self.assertSequenceEqual(tmp.get(0)([10]), functions.t_log()([10]))

        tmp.add(functions.t_sqrt())

        self.assertEqual(tmp.size, 2)
        self.assertSequenceEqual(tmp.get(0)([10]), functions.t_log()([10]))
        self.assertSequenceEqual(tmp.get(1)([10]), functions.t_sqrt()([10]))

    def test_drop_function(self):
        tmp = self.new_trans_obj()

        with self.assertRaises(IndexError):
            # Should empty, so it should raise IndexError
            tmp.drop(0)

        with self.assertRaises(TypeError):
            # Incorrect type, so should raise TypeError
            tmp.drop('1')

        tmp.add(functions.t_log1p())

        with self.assertRaises(IndexError):
            tmp.drop(1)

        tmp.drop(0)
        self.assertEqual(tmp.size, 0)

        tmp.add(functions.t_log1p())
        tmp.add(functions.t_sqrt())
        tmp.drop(0)

        self.assertSequenceEqual(tmp.get(0)([10]), functions.t_sqrt()([10]))

        tmp.drop(0)
        self.assertEqual(tmp.size, 0)

    def test_drop_first_function(self):
        tmp = self.new_trans_obj()
        tmp.add(functions.t_log1p())
        tmp.add(functions.t_sqrt())

        tmp.drop_first()
        self.assertEqual(tmp.size, 1)
        self.assertSequenceEqual(tmp.get(0)([10]), functions.t_sqrt()([10]))

    def test_drop_last_function(self):
        tmp = self.new_trans_obj()
        tmp.add(functions.t_log1p())
        tmp.add(functions.t_sqrt())
        tmp.add(functions.t_log())

        tmp.drop_last()
        self.assertEqual(tmp.size, 2)
        self.assertSequenceEqual(tmp.get(0)([10]), functions.t_log1p()([10]))

    def test_insert_function(self):
        tmp = self.new_trans_obj()
        tmp.add(functions.t_log1p())
        tmp.add(functions.t_sqrt())
        tmp.add(functions.t_log())

        tmp.insert_before(functions.t_log10(), 1)

        self.assertNotEqual(tmp.get(1)([10]), functions.t_sqrt()([10]))
        self.assertSequenceEqual(tmp.get(2)([10]), functions.t_sqrt()([10]))

    def test_insert_first_function(self):
        tmp = self.new_trans_obj()
        tmp.add(functions.t_log1p())
        tmp.add(functions.t_sqrt())
        tmp.add(functions.t_log())

        tmp.insert_first(functions.t_sqrt())

        self.assertNotEqual(tmp.get(0)([10]), functions.t_log1p()([10]))
        self.assertSequenceEqual(tmp.get(0)([10]), functions.t_sqrt()([10]))

    def test_insert_last_function(self):
        tmp = self.new_trans_obj()
        tmp.add(functions.t_log1p())
        tmp.add(functions.t_sqrt())
        tmp.add(functions.t_log())

        tmp.insert_last(functions.t_sqrt())
        self.assertNotEqual(tmp.get(-1)([10]), functions.t_log()([10]))
        self.assertSequenceEqual(tmp.get(-1)([10]), functions.t_sqrt()([10]))

    def test_clear_function(self):
        tmp = self.new_trans_obj()
        tmp.add(functions.t_sqrt())
        tmp.add(functions.t_log())

        self.assertEqual(tmp.size, 2)

        tmp.clear()
        self.assertEqual(tmp.size, 0)

    def test_apply(self):
        tmp = self.new_trans_obj()

        dat_series = numpy.ones((10,), dtype=numpy.int64)
        dat_pseries = pandas.Series(dat_series)

        self.assertIsInstance(tmp.apply(dat_series), numpy.ndarray)
        self.assertIsInstance(tmp.apply(dat_pseries), pandas.Series)

        # For cases where no transformation is performed and input is
        # either a list or tuple, check to make sure it returns a ndarray
        self.assertIsInstance(tmp.apply(list(dat_pseries)), numpy.ndarray)

        # Check if values are passed through exactly when no trans. present
        self.assertTrue(numpy.alltrue(tmp.apply(dat_series) == dat_series))

        # Do the same as above, but for pandas Series
        series_all_true = tmp.apply(dat_pseries) == dat_pseries
        self.assertTrue(pandas.Series.all(series_all_true))

        # Start creating some trans function
        tranfunc1 = functions.t_log1p()
        tranfunc2 = functions.t_sqrt()

        tmp.add(tranfunc1)

        # Check the return type when actual funs are applied
        self.assertIsInstance(tmp.apply(dat_series), numpy.ndarray)
        self.assertIsInstance(tmp.apply(dat_pseries), pandas.Series)
        self.assertIsInstance(tmp.apply(list(dat_series)), numpy.ndarray)

        # Check return values
        series_all_true = tmp.apply(dat_series) == tranfunc1(dat_series)
        self.assertTrue(numpy.alltrue(series_all_true))
        series_all_true = tmp.apply(dat_pseries) == tranfunc1(dat_pseries)
        self.assertTrue(pandas.Series.all(series_all_true))
        series_all_true = (tmp.apply(list(dat_pseries)) ==
                           tranfunc1(dat_pseries))
        self.assertTrue(numpy.alltrue(series_all_true))

        tmp.add(tranfunc2)

        # For multi-funcs
        series_all_true = (tmp.apply(dat_series) ==
                           tranfunc2(tranfunc1(dat_series)))
        self.assertTrue(numpy.alltrue(series_all_true))

        series_all_true = (tmp.apply(dat_pseries) ==
                           tranfunc2(tranfunc1(dat_pseries)))
        self.assertTrue(pandas.Series.all(series_all_true))

        series_all_true = (tmp.apply(list(dat_series)) ==
                           tranfunc2(tranfunc1(dat_series)))
        self.assertTrue(numpy.alltrue(series_all_true))


class TestInstanceCheckers(unittest.TestCase):

    def test_sequence_checker(self):
        is_sequence = progutils.is_sequence

        # These should be True
        self.assertTrue(is_sequence(set()))
        self.assertTrue(is_sequence(tuple()))
        self.assertTrue(is_sequence(list()))
        self.assertTrue(is_sequence(numpy.array([1])))

        # Should be False
        self.assertFalse(is_sequence('1'))
        self.assertFalse(is_sequence(iter([1, 3, 4])))

    def test_tuple_list_checker(self):
        is_tuple_or_list = progutils.is_tuple_or_list

        # These should be True
        self.assertTrue(is_tuple_or_list(tuple()))
        self.assertTrue(is_tuple_or_list(list()))

        # Should be False
        self.assertFalse(is_tuple_or_list('1'))
        self.assertFalse(is_tuple_or_list(iter([1, 3, 4])))
        self.assertFalse(is_tuple_or_list(set()))
        self.assertFalse(is_tuple_or_list(numpy.array([1])))

    def test_isinstance_sequence_checker(self):
        isinstance_sequence = progutils.isinstance_sequence

        # These should be True
        self.assertTrue(isinstance_sequence(['a', 'b', 'c'], str))
        self.assertTrue(isinstance_sequence(set((1, 5, 4)), int))
        self.assertTrue(isinstance_sequence(numpy.array((1.0, 5., 4.)), float))

        # Should be False
        self.assertFalse(isinstance_sequence(['a', 'b', 1], str))
        self.assertFalse(isinstance_sequence(numpy.array((1.0, 'a')), int))

    def test_callable_sequence(self):
        callable_sequence = progutils.callable_sequence

        # These should be True
        self.assertTrue(callable_sequence([numpy.mean, functions.t_log()]))
        self.assertTrue(callable_sequence((numpy.mean, numpy.sum)))

        # Should be False
        self.assertFalse(callable_sequence([3, 4]))
        self.assertFalse(callable_sequence([numpy.mean, 4]))


if __name__ == '__main__':
    unittest.main()
