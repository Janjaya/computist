"""
Copyright (c) 2021 Jan William Johnsen

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from imblearn.over_sampling import SMOTE, BorderlineSMOTE, RandomOverSampler
from imblearn.over_sampling import KMeansSMOTE, SVMSMOTE, SMOTENC
from imblearn.under_sampling import CondensedNearestNeighbour
from imblearn.under_sampling import EditedNearestNeighbours
from imblearn.under_sampling import RepeatedEditedNearestNeighbours, AllKNN
from imblearn.under_sampling import InstanceHardnessThreshold, NearMiss
from imblearn.under_sampling import NeighbourhoodCleaningRule
from imblearn.under_sampling import OneSidedSelection, RandomUnderSampler
from imblearn.under_sampling import TomekLinks
from imblearn.combine import SMOTETomek, SMOTEENN


class ImbalancedMixin():
    def _get_sampling_technique(self, type="", random_state=1, n_jobs=1):
        """Return sampling technique type"""
        # TODO: Implement a way for non-multiprocessing to use multiple jobs.
        sampling = {
            # Oversampling
            "smote": SMOTE,
            "borderlinesmote": BorderlineSMOTE,
            "smotenc": SMOTENC,
            "kmeanssmote": KMeansSMOTE,
            "svmsmote": SVMSMOTE,
            "ros": RandomOverSampler,
            # Undersampling
            "cnn": CondensedNearestNeighbour,
            "enn": EditedNearestNeighbours,
            "renn": RepeatedEditedNearestNeighbours,
            "aknn": AllKNN,
            "iht": InstanceHardnessThreshold,
            "nearmiss": NearMiss,
            "ncr": NeighbourhoodCleaningRule,
            "oss": OneSidedSelection,
            "rus": RandomUnderSampler,
            "tomelinks": TomekLinks,
            # Over- and undersampling
            "smotetomek": SMOTETomek,
            "smoteenn": SMOTEENN,
        }
        try:
            if not hasattr(sampling[type], "random_state"):
                return sampling[type](random_state=random_state)
            return sampling[type]()
        except KeyError:
            raise ValueError("No compatible sampling 'type' was provided.")

    def run_sampling(self, sampling="", X_train=None, y_train=None):
        """Resample training data using sampling technique"""
        if sampling is None:
            return (X_train, y_train)
        sampling_model = self._get_sampling_technique(sampling)
        self.verbose(f"Reshaping using '{sampling}' technique.")
        return sampling_model.fit_resample(X_train, y_train)

    def sampling_techniques(self, oversampling, undersampling):
        """Generator for over- and/or undersampling techniques"""
        sampling_techniques = [None]
        if oversampling and undersampling:
            sampling_techniques = ["smotetomek", "smoteenn"]
        elif oversampling:
            sampling_techniques = ["ros", "borderlinesmote", "kmeanssmote", "svmsmote", "smote"]
        elif undersampling:
            sampling_techniques = ["cnn", "enn", "renn", "aknn", "iht", "nearmiss", "ncr", "oss", "rus", "tomelinks"]
        for sampling in sampling_techniques:
            yield sampling
