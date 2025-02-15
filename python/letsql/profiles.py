from pathlib import Path
import yaml  # noqa: F401
from typing import Callable, Optional
from letsql.backends.let import Backend

from pathlib import Path
import yaml  # noqa: F401
from typing import Callable, Optional
from letsql.backends.let import Backend

class ManageProfiles:
    def __init__(self, config_path: str, connect_method: Optional[Callable] = None):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            self.init_profiles()
        with open(self.config_path, "r") as file:
            self.profiles = yaml.safe_load(file)

        self._connect_method = connect_method
    #TODO: still just uses the path to file
    #so profile object must be saved before it can be used 
    def connect(self, profile_name: str) -> Backend:
        """Connect using a profile name.
        """
        if self._connect_method is None:
            raise ValueError("No connect method provided. Initialize with connect_method")
        return self._connect_method(profile_name, self.config_path)

    def __iter__(self):
        return iter(self.profiles["profiles"])
    def __eq__(self, other):
        return self.profiles == other.profiles

    def __repr__(self):
        return f"ManageProfiles({self.config_path})"

    def connect(self, profile_name: str) -> Backend:
        """Connect using a profile name.
        """
        if self._connect_method is None:
            raise ValueError("No connect method provided. Initialize with connect_method")
        return self._connect_method(profile_name, self.config_path)

    def init_profiles(self):
        """Initialize the profiles.yaml file with default profiles."""
        if self.config_path.exists():
            raise FileExistsError(f"The file '{self.config_path}' already exists.")
        print(f"creating profile @ {self.config_path}")
        default_profiles = {
            "profiles": {
                "default":{
                    "backend": "letsql"
                }
 
            }
        }
        
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config_path.open("w") as file:
            yaml.dump(default_profiles, file)
        self.profiles = default_profiles
        return default_profiles
    # Just returning what it did but we can also return the profiles object or nothing 
    def add_profile(self, profile_name: str, profile_kwargs: dict):
        """Create a new profile in the profiles.yaml file using Jinja2."""
        if profile_name in self.profiles["profiles"]:
            raise ValueError(f"Profile '{profile_name}' already exists in {self.config_path}")

        # Directly add the profile without using Jinja2 template
        self.profiles["profiles"][profile_name] = profile_kwargs
        return self.profiles["profiles"][profile_name]

    def deep_merge(self, base_dict: dict, update_dict: dict) -> dict:
        #dont know if we need this but we got it 
        """Deep merge two dictionaries."""
        merged = base_dict.copy()
        for key, value in update_dict.items():
            if (
                key in merged 
                and isinstance(merged[key], dict) 
                and isinstance(value, dict)
            ):
                merged[key] = self.deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged

    def update_profile(self, profile_name: str, profile_kwargs: dict):
        """Update an existing profile in the profiles.yaml file."""
        if profile_name not in self.profiles["profiles"]:
            raise ValueError(f"Profile '{profile_name}' does not exist in {self.config_path}")

        current_profile = self.profiles["profiles"][profile_name]
        merged_profile = self.deep_merge(current_profile, profile_kwargs)
        self.profiles["profiles"][profile_name] = merged_profile
        
        return self.profiles["profiles"][profile_name]

    def delete_profile(self, profile_name: str):
        """Delete an existing profile from the profiles.yaml file."""
        if profile_name not in self.profiles["profiles"]:
            raise ValueError(f"Profile '{profile_name}' does not exist in {self.config_path}")

        deleted_profile = self.profiles["profiles"].pop(profile_name)
        return deleted_profile

    def save_profiles(self):
        """Write all changes made to the profiles object to the profiles.yaml file."""
        with open(self.config_path, "w") as file:
            yaml.dump(self.profiles, file)
   
        